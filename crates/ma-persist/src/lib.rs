//! Durable history: normalised events to Parquet, rolled hourly, behind an
//! object store.
//!
//! # Where this sits relative to the tape
//!
//! CLAUDE.md §4 says replay is **two layers, not one**, and that neither
//! replaces the other. This crate is the second layer. The first —
//! `ma_pipeline::tape` — records raw bytes before parsing, which is the only
//! way a recorded session can reproduce a parser bug or a venue schema change.
//! This one records what those bytes were understood to mean.
//!
//! |                     | raw-frame tape        | Parquet (this crate)      |
//! |---------------------|-----------------------|---------------------------|
//! | Records             | bytes, pre-parse      | normalised events         |
//! | Reproduces          | parser bugs, drift    | books, checksum checks    |
//! | Horizon             | minutes, by hand      | hours, continuously       |
//! | Queryable           | `less`                | any Parquet engine        |
//! | Written by          | the ingest task       | the aggregator's tee      |
//!
//! The row that matters is the last one. A tape is teed off ingest, *before*
//! the venue state machines; Parquet is teed off the aggregator, *after* them.
//! That is why Parquet cannot reproduce a parser bug and why it can be trusted
//! to describe the same books the live run served — it is the same event
//! sequence those books were built from, not a second derivation of it.
//!
//! # Crate boundaries
//!
//! `arrow` and `parquet` appear here and in no other crate, the same
//! discipline that keeps `tokio` out of `ma-core` and a transport out of
//! `ma-venues`. `ma-pipeline` should be buildable without a columnar format
//! library, and this crate should be replaceable without touching it.
//!
//! # AWS
//!
//! The `s3` feature is **off by default**, so a default build neither compiles
//! nor links the AWS SDK. That is CLAUDE.md's sequencing rule made structural:
//! nothing reaches S3 before an IAM user scoped to one bucket prefix exists,
//! and the offline suite cannot acquire a dependency on credentials by
//! accident. See [`store`] and, when the feature is on, [`s3`].

pub mod reader;
pub mod schema;
pub mod store;
pub mod writer;

#[cfg(feature = "s3")]
pub mod s3;

pub use reader::{EventReader, ReadError, StoredEvent};
pub use store::{LocalStore, ObjectStore, StoreError};
pub use writer::{EventWriter, WriteError, WriterConfig, run};

/// The default key namespace for the event archive.
pub const DEFAULT_PREFIX: &str = "events";

/// Build a store from an operator-supplied URI.
///
/// `s3://bucket/prefix` needs the `s3` feature; anything else is a local path.
/// The failure when the feature is off is deliberately a clear message rather
/// than a silent fallback to the local disk — an operator who asked for S3 and
/// got a directory would not find out until they went looking for data that
/// was never uploaded.
///
/// # Errors
/// If the URI names a backend this build does not have.
pub async fn store_from_uri(uri: &str) -> Result<std::sync::Arc<dyn ObjectStore>, StoreError> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        #[cfg(feature = "s3")]
        {
            return s3::S3Store::from_uri(rest)
                .await
                .map(|s| std::sync::Arc::new(s) as _);
        }
        #[cfg(not(feature = "s3"))]
        {
            let _ = rest;
            return Err(StoreError::Config(format!(
                "this build has no S3 support: {uri:?} needs `--features s3`. \
                 Nothing writes to S3 until an IAM user scoped to one bucket \
                 prefix exists — see CLAUDE.md."
            )));
        }
    }
    Ok(std::sync::Arc::new(LocalStore::new(uri)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_plain_path_is_a_local_store() {
        let store = store_from_uri("/tmp/market-data").await.unwrap();
        assert_eq!(store.describe(), "local:/tmp/market-data");
    }

    #[cfg(not(feature = "s3"))]
    #[tokio::test]
    async fn asking_for_s3_without_the_feature_fails_loudly() {
        // The failure that must not be a silent fallback: an operator who
        // asked for S3 and quietly got a local directory finds out when they
        // go looking for data that was never uploaded.
        let err = store_from_uri("s3://my-bucket/market-data")
            .await
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("--features s3"), "{message}");
    }
}
