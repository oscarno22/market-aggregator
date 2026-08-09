//! Where finished files go.
//!
//! One trait with two implementations: the local filesystem, which the offline
//! suite uses and which is a perfectly good production target for a single
//! node, and S3, which is behind the `s3` feature and off by default.
//!
//! # Why this is a trait rather than an `if` on a config flag
//!
//! The same reason `ma_pipeline::net::Network` is a trait: it is the seam that
//! lets every test above it run with no network and no credentials. A Parquet
//! writer that talks to S3 directly could only be tested by talking to S3,
//! which would make the durability layer the one part of the system the
//! offline suite could not reach — in a project whose stated rule is that
//! anything which cannot be tested in replay mode is not done.
//!
//! # Why the futures are boxed here and not in `Network`
//!
//! `Network` is used generically: exactly one implementation is chosen at
//! compile time per binary, so `-> impl Future` costs nothing and stays
//! `Send`-checked. A store is chosen at *runtime*, from a URI the operator
//! passed, so it has to be a trait object — and `impl Future` in return
//! position is not dyn-safe. Boxing is the price of that choice, and it is
//! paid once per finished file rather than once per market tick, which is why
//! it is the right trade here and would be the wrong one there.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("i/o error at {key}: {source}")]
    Io {
        key: String,
        #[source]
        source: std::io::Error,
    },
    #[error("object store rejected {key}: {message}")]
    Rejected { key: String, message: String },
    #[error("{0}")]
    Config(String),
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Somewhere finished Parquet files can be put and got back.
///
/// Deliberately minimal. This is not an abstraction over object stores in
/// general — it is the four operations the persistence layer performs, which
/// is what keeps a local directory a first-class implementation rather than a
/// test double pretending to be S3.
pub trait ObjectStore: std::fmt::Debug + Send + Sync {
    /// A human-readable description of where this store writes, for logs and
    /// for the startup line that tells an operator what they just pointed at.
    fn describe(&self) -> String;

    fn put(&self, key: &str, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), StoreError>>;

    fn get(&self, key: &str) -> BoxFuture<'_, Result<Vec<u8>, StoreError>>;

    /// Keys under `prefix`, sorted. Used by Parquet replay to find the files
    /// making up a session, in order.
    fn list(&self, prefix: &str) -> BoxFuture<'_, Result<Vec<String>, StoreError>>;
}

/// A directory on this machine.
///
/// Not a stand-in for the "real" store: for a single-node aggregator this is a
/// complete answer, and S3 is the option you take when you want the data
/// somewhere the node is not.
#[derive(Clone, Debug)]
pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Resolve a key beneath the root, refusing anything that would escape it.
    ///
    /// Keys are generated internally from timestamps, so traversal is not a
    /// live threat today. It is checked anyway because the day a key comes
    /// from a request parameter, this is the code that will already be in
    /// place — and because a `..` in a key is a bug worth failing on whatever
    /// its origin.
    fn resolve(&self, key: &str) -> Result<PathBuf, StoreError> {
        let rejected = |message: &str| StoreError::Rejected {
            key: key.to_owned(),
            message: message.to_owned(),
        };
        if key.is_empty() {
            return Err(rejected("empty key"));
        }
        let path = Path::new(key);
        if path.is_absolute() {
            return Err(rejected("absolute keys would escape the store root"));
        }
        for part in path.components() {
            match part {
                std::path::Component::Normal(_) => {}
                _ => return Err(rejected("key contains a traversal or root component")),
            }
        }
        Ok(self.root.join(path))
    }
}

impl ObjectStore for LocalStore {
    fn describe(&self) -> String {
        format!("local:{}", self.root.display())
    }

    fn put(&self, key: &str, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), StoreError>> {
        let key = key.to_owned();
        Box::pin(async move {
            let path = self.resolve(&key)?;
            let io = |source| StoreError::Io {
                key: key.clone(),
                source,
            };
            if let Some(dir) = path.parent() {
                tokio::fs::create_dir_all(dir).await.map_err(io)?;
            }
            // Write to a temporary name and rename into place. A reader that
            // lists the directory must never see a half-written Parquet file:
            // the footer is at the *end*, so a truncated file is not a short
            // file, it is an unreadable one. Rename within a directory is
            // atomic on every filesystem this runs on.
            let staging = path.with_extension("partial");
            tokio::fs::write(&staging, &bytes).await.map_err(io)?;
            tokio::fs::rename(&staging, &path).await.map_err(io)
        })
    }

    fn get(&self, key: &str) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        let key = key.to_owned();
        Box::pin(async move {
            let path = self.resolve(&key)?;
            tokio::fs::read(&path)
                .await
                .map_err(|source| StoreError::Io {
                    key: key.clone(),
                    source,
                })
        })
    }

    fn list(&self, prefix: &str) -> BoxFuture<'_, Result<Vec<String>, StoreError>> {
        let prefix = prefix.to_owned();
        Box::pin(async move {
            let mut out = Vec::new();
            let root = self.root.clone();
            walk(&root, &root, &prefix, &mut out).await?;
            out.sort();
            Ok(out)
        })
    }
}

/// Recursive directory walk, collecting store-relative keys under `prefix`.
///
/// Written as an explicit stack rather than a recursive `async fn`, which
/// would need boxing at every level for no benefit.
async fn walk(
    root: &Path,
    start: &Path,
    prefix: &str,
    out: &mut Vec<String>,
) -> Result<(), StoreError> {
    let mut stack = vec![start.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            // A store that has never been written to has no root directory.
            // "Nothing there yet" is an empty listing, not an error.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(StoreError::Io {
                    key: dir.display().to_string(),
                    source,
                });
            }
        };

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|source| StoreError::Io {
                key: dir.display().to_string(),
                source,
            })?
        {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            // Half-written files are invisible until renamed into place.
            if path.extension().is_some_and(|e| e == "partial") {
                continue;
            }
            if let Ok(rel) = path.strip_prefix(root) {
                let key = rel.to_string_lossy().replace('\\', "/");
                if key.starts_with(prefix) {
                    out.push(key);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_put_object_comes_back_byte_identical() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::new(dir.path());

        let bytes = vec![0_u8, 1, 2, 250, 251];
        store
            .put(
                "events/date=2026-08-09/hour=03/part-00000.parquet",
                bytes.clone(),
            )
            .await
            .unwrap();

        let got = store
            .get("events/date=2026-08-09/hour=03/part-00000.parquet")
            .await
            .unwrap();
        assert_eq!(got, bytes);
    }

    #[tokio::test]
    async fn listing_is_sorted_and_prefix_filtered() {
        // Replay reads files in order, and "in order" here means lexicographic
        // over date=/hour= keys — which is chronological by construction. An
        // unsorted listing would replay an hour out of sequence and produce a
        // book from the future.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::new(dir.path());

        for key in [
            "events/date=2026-08-09/hour=04/part-00000.parquet",
            "events/date=2026-08-09/hour=03/part-00000.parquet",
            "events/date=2026-08-10/hour=00/part-00000.parquet",
            "other/thing.txt",
        ] {
            store.put(key, b"x".to_vec()).await.unwrap();
        }

        let keys = store.list("events/").await.unwrap();
        assert_eq!(
            keys,
            [
                "events/date=2026-08-09/hour=03/part-00000.parquet",
                "events/date=2026-08-09/hour=04/part-00000.parquet",
                "events/date=2026-08-10/hour=00/part-00000.parquet",
            ],
            "listing was not chronological, or leaked a key outside the prefix"
        );
    }

    #[tokio::test]
    async fn listing_an_empty_store_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::new(dir.path().join("never-written"));
        assert!(store.list("events/").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn keys_cannot_escape_the_store_root() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::new(dir.path().join("root"));

        for bad in ["../outside", "a/../../outside", "/etc/passwd", ""] {
            let err = store.put(bad, b"x".to_vec()).await.unwrap_err();
            assert!(
                matches!(err, StoreError::Rejected { .. }),
                "{bad:?} was accepted: {err}"
            );
        }
    }

    #[tokio::test]
    async fn a_partially_written_file_is_never_listed() {
        // A Parquet footer is at the end of the file, so a reader that sees a
        // half-written one does not get truncated data — it gets an unreadable
        // file and a failed run. The staging-then-rename in `put` is what makes
        // a listing safe to consume while a writer is active.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::new(dir.path());
        store
            .put("events/part-00000.parquet", b"done".to_vec())
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("events/part-00001.partial"), b"half")
            .await
            .unwrap();

        assert_eq!(
            store.list("events/").await.unwrap(),
            ["events/part-00000.parquet"]
        );
    }

    #[tokio::test]
    async fn a_store_can_be_used_as_a_trait_object() {
        // The whole reason the futures are boxed: the store is chosen at
        // runtime from an operator-supplied URI, so it has to be `dyn`.
        let dir = tempfile::tempdir().unwrap();
        let store: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(LocalStore::new(dir.path()));
        store.put("k", b"v".to_vec()).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), b"v");
        assert!(store.describe().starts_with("local:"));
    }
}
