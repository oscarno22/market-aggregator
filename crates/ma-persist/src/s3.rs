//! S3 as an [`ObjectStore`], behind the `s3` feature.
//!
//! # Almost nothing happens here, and that is the point
//!
//! Everything that decides whether this process may talk to a bucket at all —
//! the mandatory prefix, the `MA_S3_ACK_SCOPED_IAM` interlock, and the scope
//! probe that *verifies* the IAM scoping instead of taking the operator's word
//! for it — lives in [`ma_aws`], not here. This file is the adapter that makes
//! a [`ScopedBucket`] look like the four operations the persistence layer
//! performs.
//!
//! It was not always split. v2 wrote the check inline, which was right while
//! one subsystem wanted a bucket. v4's cluster registry is a second, and the
//! choice was then between duplicating the project's only credential control
//! or extracting it. `ma_aws`'s crate docs carry the argument, and the whole of
//! it is that a control existing twice will disagree with itself eventually —
//! not on the day it is copied.
//!
//! # Status
//!
//! Exercised end to end against a real bucket on 2026-08-09: written under
//! `s3://…/events`, flushed 58,574 rows on `SIGTERM`, and replayed back out
//! into three live checksum-verified books. See `docs/DESIGN.md` §10 for what
//! that established and what it left untested.

use ma_aws::{AwsError, ScopedBucket};

use crate::store::{ObjectStore, StoreError};

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// An S3 bucket, confined to one key prefix.
#[derive(Clone, Debug)]
pub struct S3Store {
    bucket: ScopedBucket,
}

impl S3Store {
    /// Split `bucket/prefix` (the part after `s3://`).
    ///
    /// # Errors
    /// If the bucket or the prefix is missing.
    pub fn parse_uri(rest: &str) -> Result<(String, String), StoreError> {
        ScopedBucket::parse_uri(rest).map_err(into_store_error)
    }

    /// Parse and connect in one step. `rest` is the part after `s3://`.
    ///
    /// # Errors
    /// As [`ScopedBucket::from_uri`].
    pub async fn from_uri(rest: &str) -> Result<Self, StoreError> {
        ScopedBucket::from_uri(rest)
            .await
            .map(|bucket| Self { bucket })
            .map_err(into_store_error)
    }

    /// Connect and verify the credential scope before returning.
    ///
    /// # Errors
    /// As [`ScopedBucket::connect`].
    pub async fn connect(bucket: &str, prefix: &str) -> Result<Self, StoreError> {
        ScopedBucket::connect(bucket, prefix)
            .await
            .map(|bucket| Self { bucket })
            .map_err(into_store_error)
    }
}

/// `AwsError` carries the same two shapes `StoreError` does, so the mapping is
/// total and keeps the distinction the caller acts on: a `Config` failure is
/// fatal at startup, a `Rejected` one is a single operation that failed.
fn into_store_error(e: AwsError) -> StoreError {
    match e {
        AwsError::Config(message) => StoreError::Config(message),
        AwsError::Rejected { key, message } => StoreError::Rejected { key, message },
    }
}

impl ObjectStore for S3Store {
    fn describe(&self) -> String {
        self.bucket.describe()
    }

    fn put(&self, key: &str, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), StoreError>> {
        let key = key.to_owned();
        Box::pin(async move { self.bucket.put(&key, bytes).await.map_err(into_store_error) })
    }

    fn get(&self, key: &str) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        let key = key.to_owned();
        Box::pin(async move { self.bucket.get(&key).await.map_err(into_store_error) })
    }

    fn list(&self, prefix: &str) -> BoxFuture<'_, Result<Vec<String>, StoreError>> {
        let prefix = prefix.to_owned();
        Box::pin(async move { self.bucket.list(&prefix).await.map_err(into_store_error) })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_without_a_prefix_is_refused_through_this_layer_too() {
        // The check itself is `ma_aws`'s and is tested there. What this asserts
        // is that the adapter does not lose it — a `parse_uri` that quietly
        // accepted a bare bucket here would defeat the control regardless of
        // how well the shared crate implements it.
        let err = S3Store::parse_uri("my-bucket").unwrap_err();
        assert!(matches!(err, StoreError::Config(_)), "{err}");
        assert!(err.to_string().contains("names no prefix"), "{err}");
    }

    #[test]
    fn a_configuration_failure_stays_a_configuration_failure() {
        // The distinction the caller acts on: `Config` is fatal at startup,
        // `Rejected` is one operation that failed. Collapsing them would make
        // an unscoped credential look like a transient write error, which the
        // writer's `run` loop deliberately logs and continues past.
        let config = into_store_error(AwsError::Config("nope".to_owned()));
        assert!(matches!(config, StoreError::Config(_)));

        let rejected = into_store_error(AwsError::Rejected {
            key: "events/x.parquet".to_owned(),
            message: "slow down".to_owned(),
        });
        assert!(matches!(rejected, StoreError::Rejected { .. }));
    }
}
