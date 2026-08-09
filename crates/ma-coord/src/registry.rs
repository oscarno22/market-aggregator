//! Two places lease records can live.
//!
//! Both are complete implementations rather than one real one and one test
//! double, for the same reason `LocalStore` is not a stand-in for S3: the
//! operation set is small enough — write my key, read every key, delete my key
//! — that a directory satisfies it exactly, and pretending otherwise would put
//! the coordination layer's only real implementation behind a network the
//! offline suite cannot reach.
//!
//! # Why a plain directory is enough
//!
//! Because no node ever writes a key another node writes. There is no
//! read-modify-write anywhere in [`crate::lease`], so there is nothing to make
//! atomic between nodes — only the single-file write has to be atomic against
//! a *reader*, which `write to temp, rename` gives on any POSIX filesystem.
//!
//! That is also the reason this shape ports to an object store unchanged: it
//! needs `PutObject`, `ListObjects` and `DeleteObject`, and specifically *not*
//! conditional writes. The absence of a compare-and-swap in the trait is the
//! design, not an omission.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::assign::NodeId;
use crate::lease::{BoxFuture, Lease, Registry, RegistryError};

/// A shared directory: one JSON file per node, named after it.
///
/// Suitable for several processes on one host, or several hosts sharing a
/// network filesystem. Not suitable across a partition that makes the
/// filesystem *silently* stale rather than unavailable — but nothing is, and
/// the holder-side expiry in [`crate::lease`] is what bounds the damage when
/// it happens.
#[derive(Clone, Debug)]
pub struct DirRegistry {
    root: PathBuf,
}

impl DirRegistry {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// One file per node. The name is sanitised rather than trusted: a node id
    /// arrives from a command line, and `--node-id ../../etc/passwd` must be a
    /// strange filename rather than a path traversal. Same argument as
    /// `LocalStore::resolve` — checked now because the day it matters is not
    /// the day anyone will think to add it.
    fn path_for(&self, node: &NodeId) -> PathBuf {
        let safe: String = node
            .as_str()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.root.join(format!("{safe}.json"))
    }
}

impl Registry for DirRegistry {
    fn describe(&self) -> String {
        format!("directory {}", self.root.display())
    }

    fn announce(&self, lease: &Lease) -> BoxFuture<'_, Result<(), RegistryError>> {
        let path = self.path_for(&lease.node);
        let root = self.root.clone();
        // Serialising a `Lease` cannot fail — it is plain data with no map
        // keys — so this is not an error path worth widening the enum for.
        let body = serde_json::to_vec(lease).unwrap_or_default();
        Box::pin(async move {
            tokio::fs::create_dir_all(&root).await?;
            // Write-then-rename, so a reader never sees a half-written record.
            // A torn lease would deserialise as malformed and be skipped,
            // which reads as "that node is gone" — the one conclusion that
            // lets someone else take its streams.
            let tmp = path.with_extension("tmp");
            tokio::fs::write(&tmp, &body).await?;
            tokio::fs::rename(&tmp, &path).await?;
            Ok(())
        })
    }

    fn members(&self) -> BoxFuture<'_, Result<Vec<Lease>, RegistryError>> {
        let root = self.root.clone();
        Box::pin(async move {
            let mut out = Vec::new();
            let mut dir = match tokio::fs::read_dir(&root).await {
                Ok(dir) => dir,
                // No directory yet means no members yet, which is the true
                // answer for the first node to start. An error here would make
                // an empty cluster indistinguishable from an unreachable one.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                if path.extension().is_none_or(|e| e != "json") {
                    continue;
                }
                let bytes = match tokio::fs::read(&path).await {
                    Ok(bytes) => bytes,
                    // A record deleted between listing and reading is a node
                    // that withdrew mid-scan. Not an error, and not a reason
                    // to discard the members already read.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                match serde_json::from_slice::<Lease>(&bytes) {
                    Ok(lease) => out.push(lease),
                    Err(source) => {
                        return Err(RegistryError::Malformed {
                            node: path.display().to_string(),
                            source,
                        });
                    }
                }
            }
            Ok(out)
        })
    }

    fn withdraw(&self, node: &NodeId) -> BoxFuture<'_, Result<(), RegistryError>> {
        let path = self.path_for(node);
        Box::pin(async move {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            }
        })
    }
}

/// An in-process registry.
///
/// Exists so the offline suite can run several coordinators against one
/// registry and step them in whatever interleaving it wants — which is how the
/// safety property is actually tested, since the interesting orderings are the
/// ones that occur once a week in production.
#[derive(Clone, Debug, Default)]
pub struct MemoryRegistry {
    leases: Arc<Mutex<BTreeMap<NodeId, Lease>>>,
    /// When set, every operation fails. Simulates a node partitioned from the
    /// registry while its sockets stay perfectly healthy — the case
    /// holder-side expiry exists for, and the one a real cluster cannot be
    /// asked to produce on demand.
    offline: Arc<Mutex<bool>>,
}

impl MemoryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cut this handle off from the registry. Other handles sharing the same
    /// store are unaffected, which is what makes a one-sided partition
    /// expressible.
    pub fn set_offline(&self, offline: bool) {
        if let Ok(mut flag) = self.offline.lock() {
            *flag = offline;
        }
    }

    /// A second handle onto the same store, with its own connectivity.
    pub fn handle(&self) -> Self {
        Self {
            leases: Arc::clone(&self.leases),
            offline: Arc::new(Mutex::new(false)),
        }
    }

    fn is_offline(&self) -> bool {
        self.offline.lock().map(|f| *f).unwrap_or(false)
    }
}

fn partitioned() -> RegistryError {
    RegistryError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "registry handle is offline",
    ))
}

impl Registry for MemoryRegistry {
    fn describe(&self) -> String {
        "in-process registry".to_owned()
    }

    fn announce(&self, lease: &Lease) -> BoxFuture<'_, Result<(), RegistryError>> {
        let lease = lease.clone();
        Box::pin(async move {
            if self.is_offline() {
                return Err(partitioned());
            }
            if let Ok(mut leases) = self.leases.lock() {
                leases.insert(lease.node.clone(), lease);
            }
            Ok(())
        })
    }

    fn members(&self) -> BoxFuture<'_, Result<Vec<Lease>, RegistryError>> {
        Box::pin(async move {
            if self.is_offline() {
                return Err(partitioned());
            }
            Ok(self
                .leases
                .lock()
                .map(|l| l.values().cloned().collect())
                .unwrap_or_default())
        })
    }

    fn withdraw(&self, node: &NodeId) -> BoxFuture<'_, Result<(), RegistryError>> {
        let node = node.clone();
        Box::pin(async move {
            if self.is_offline() {
                return Err(partitioned());
            }
            if let Ok(mut leases) = self.leases.lock() {
                leases.remove(&node);
            }
            Ok(())
        })
    }
}

/// Build a registry from a URI-ish string, so the binary takes one flag.
///
/// Only a path today. Kept as a function rather than inlined because the S3
/// shape is the same three operations — see the module docs — and this is
/// where that would be chosen, exactly as `ma_persist::store_from_uri` does.
pub fn registry_from_uri(uri: &str) -> Result<Box<dyn Registry>, String> {
    if uri.starts_with("s3://") {
        return Err(
            "an S3-backed cluster registry is not implemented. The trait needs only PutObject, \
             ListObjects and DeleteObject — deliberately no conditional write — so it is a small \
             addition, but nothing in this project writes to S3 before an IAM user scoped to one \
             bucket prefix exists. See CLAUDE.md."
                .to_owned(),
        );
    }
    Ok(Box::new(DirRegistry::new(Path::new(uri))))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    fn lease(node: &str, written_at_unix_ms: u64) -> Lease {
        Lease {
            node: NodeId::new(node),
            written_at_unix_ms,
            ttl_ms: 15_000,
        }
    }

    #[tokio::test]
    async fn a_directory_round_trips_every_nodes_record() {
        let dir = tempfile::tempdir().unwrap();
        let registry = DirRegistry::new(dir.path());

        registry.announce(&lease("node-a", 1000)).await.unwrap();
        registry.announce(&lease("node-b", 2000)).await.unwrap();

        let mut members = registry.members().await.unwrap();
        members.sort_by(|a, b| a.node.cmp(&b.node));
        assert_eq!(members, vec![lease("node-a", 1000), lease("node-b", 2000)]);
    }

    #[tokio::test]
    async fn announcing_again_replaces_only_that_nodes_record() {
        // The property that removes the need for a lock: renewal is a write to
        // one key, and it cannot disturb another node's.
        let dir = tempfile::tempdir().unwrap();
        let registry = DirRegistry::new(dir.path());

        registry.announce(&lease("node-a", 1000)).await.unwrap();
        registry.announce(&lease("node-b", 1000)).await.unwrap();
        registry.announce(&lease("node-a", 9999)).await.unwrap();

        let members = registry.members().await.unwrap();
        assert_eq!(members.len(), 2);
        let a = members
            .iter()
            .find(|l| l.node.as_str() == "node-a")
            .unwrap();
        let b = members
            .iter()
            .find(|l| l.node.as_str() == "node-b")
            .unwrap();
        assert_eq!(a.written_at_unix_ms, 9999);
        assert_eq!(b.written_at_unix_ms, 1000, "a renewal touched another node");
    }

    #[tokio::test]
    async fn an_empty_cluster_is_not_an_error() {
        // A missing directory is the first node's normal experience. If it
        // errored, "nobody has started yet" would be indistinguishable from
        // "the registry is unreachable" — and those must lead to opposite
        // behaviour: take the streams, or stand down.
        let dir = tempfile::tempdir().unwrap();
        let registry = DirRegistry::new(dir.path().join("not-created-yet"));
        assert!(registry.members().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn withdrawing_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let registry = DirRegistry::new(dir.path());
        registry.announce(&lease("node-a", 1000)).await.unwrap();

        registry.withdraw(&NodeId::new("node-a")).await.unwrap();
        registry.withdraw(&NodeId::new("node-a")).await.unwrap();
        assert!(registry.members().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_node_id_cannot_escape_the_registry_directory() {
        let dir = tempfile::tempdir().unwrap();
        let registry = DirRegistry::new(dir.path().join("cluster"));
        registry
            .announce(&lease("../../escaped", 1000))
            .await
            .unwrap();

        // The record landed inside the registry directory under a mangled
        // name, not two levels up.
        assert!(!dir.path().join("escaped.json").exists());
        assert_eq!(registry.members().await.unwrap().len(), 1);
    }

    #[test]
    fn a_record_from_the_future_is_treated_as_live() {
        // Clock skew, not evidence of death. Believing a record dead is what
        // starts a second subscription, so the uncertain reading has to be the
        // conservative one.
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_000);
        let future = lease("node-a", 2_000_000);
        assert!(!future.expired_at(now));
    }

    #[test]
    fn a_record_older_than_its_own_ttl_is_expired() {
        let written = 1_000_000;
        let l = lease("node-a", written);
        let just_inside = SystemTime::UNIX_EPOCH + Duration::from_millis(written + 15_000);
        let just_outside = SystemTime::UNIX_EPOCH + Duration::from_millis(written + 15_001);
        assert!(!l.expired_at(just_inside));
        assert!(l.expired_at(just_outside));
    }

    #[tokio::test]
    async fn an_offline_handle_fails_without_disturbing_the_others() {
        let shared = MemoryRegistry::new();
        let other = shared.handle();

        shared.announce(&lease("node-a", 1000)).await.unwrap();
        shared.set_offline(true);

        assert!(shared.members().await.is_err());
        assert_eq!(
            other.members().await.unwrap().len(),
            1,
            "one node's partition took the registry down for everyone"
        );
    }

    #[test]
    fn an_s3_registry_uri_is_refused_with_the_reason() {
        let e = registry_from_uri("s3://bucket/cluster").unwrap_err();
        assert!(e.contains("scoped to one bucket prefix"), "{e}");
    }
}
