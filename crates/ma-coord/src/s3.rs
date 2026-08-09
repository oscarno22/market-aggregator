//! The cluster registry in an object store, behind the `s3` feature.
//!
//! # This is the payoff for the trait having no compare-and-swap
//!
//! [`Registry`] was designed in v3 with three operations — write my key, read
//! every key, delete my key — and `registry`'s module docs said at the time
//! that the absence of a conditional write "is the design, not an omission",
//! and that the shape would port to an object store unchanged. This is that
//! claim being cashed, and it cost the operations below and nothing else: no
//! conditional `PutObject`, no `If-None-Match`, no DynamoDB table beside the
//! bucket to hold a lock.
//!
//! The reason it is that cheap is [`crate::lease`]'s argument, not S3's
//! feature set. **No node ever writes a key another node writes**, so there is
//! nothing to serialise between nodes and no read-modify-write to make atomic.
//! Had membership been a single shared document, this file would need a
//! compare-and-swap S3 did not offer until 2024 and would still be the wrong
//! primitive to lean on.
//!
//! # What S3 has to guarantee, and does
//!
//! The settling-period half of the lease argument turns on one property:
//!
//! > B's record is durable from `t_write`, so *any* successful membership read
//! > after `t_write` returns B.
//!
//! That is read-after-write and list-after-write consistency, and S3 has
//! provided both — strongly, for all requests, with no performance or
//! availability cost — since December 2020. Before that this file would have
//! been unsound rather than slow: under the old eventual-consistency model a
//! joining node could be invisible to a listing taken after it announced, and
//! the disjointness proof would quietly not hold. Worth stating explicitly,
//! because it is the sort of assumption that is inherited from a filesystem
//! and never re-examined.
//!
//! A `DeleteObject` is likewise immediately reflected in a subsequent listing,
//! which is what makes a clean withdrawal on `SIGTERM` actually save the rest
//! of the cluster a lease's worth of unowned streams.
//!
//! # What it does not guarantee, and what that costs
//!
//! Latency. A registry round trip is now a network call rather than a `read`
//! on a local filesystem, so `LeaseConfig::renew` has to leave room for it.
//! The lease loop already treats a failed round trip as a reason to stand down
//! rather than to retry harder, so the failure mode is the safe one — a node
//! that cannot reach S3 releases its streams — but a `ttl` tuned for a shared
//! directory will make an S3-backed cluster flap. `docs/DESIGN.md` §13 has the
//! numbers.

use ma_aws::{AwsError, ScopedBucket};
use tracing::warn;

use crate::assign::NodeId;
use crate::lease::{BoxFuture, Lease, Registry, RegistryError};
use crate::registry::record_name;

/// One object per node, under a bucket prefix.
#[derive(Clone, Debug)]
pub struct S3Registry {
    bucket: ScopedBucket,
}

impl S3Registry {
    /// Connect to `bucket/prefix` (the part after `s3://`), verifying the
    /// credential scope before returning.
    ///
    /// # Errors
    /// As [`ScopedBucket::from_uri`]: a missing prefix, the unset
    /// acknowledgement, credentials that reach outside the prefix, or a bucket
    /// that cannot be reached at all.
    pub async fn from_uri(rest: &str) -> Result<Self, AwsError> {
        Ok(Self {
            bucket: ScopedBucket::from_uri(rest).await?,
        })
    }
}

/// Every S3 failure reaching the lease loop is one thing to that loop: the
/// round trip did not complete, so the hold deadline is not extended and the
/// node is that much closer to standing down. Flattening to `Io` keeps that
/// single meaning rather than inviting a caller to treat some subset as
/// recoverable — which would be the one way to reintroduce "renew a lease you
/// can no longer be told to release".
fn into_registry_error(e: AwsError) -> RegistryError {
    RegistryError::Io(std::io::Error::other(e.to_string()))
}

impl Registry for S3Registry {
    fn describe(&self) -> String {
        self.bucket.describe()
    }

    fn announce(&self, lease: &Lease) -> BoxFuture<'_, Result<(), RegistryError>> {
        let key = record_name(&lease.node);
        // Serialising a `Lease` cannot fail — plain data, no map keys — so this
        // is not an error path worth widening the enum for. Same call as
        // `DirRegistry::announce`.
        let body = serde_json::to_vec(lease).unwrap_or_default();
        Box::pin(async move {
            self.bucket
                .put(&key, body)
                .await
                .map_err(into_registry_error)
        })
    }

    fn members(&self) -> BoxFuture<'_, Result<Vec<Lease>, RegistryError>> {
        Box::pin(async move {
            let keys = self.bucket.list("").await.map_err(into_registry_error)?;
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                if !key.ends_with(".json") {
                    continue;
                }
                let bytes = match self.bucket.get(&key).await {
                    Ok(bytes) => bytes,
                    // A record deleted between the listing and the read is a
                    // node that withdrew mid-scan. Not an error, and not a
                    // reason to discard the members already read — the same
                    // conclusion `DirRegistry` draws from `NotFound`.
                    //
                    // Unlike the filesystem there is no typed "missing" here,
                    // so this warns rather than silently skipping: a read that
                    // fails for some *other* reason would otherwise shrink the
                    // membership this node computes its assignment from, which
                    // is how a stream ends up owned twice.
                    Err(e) => {
                        warn!(%key, error = %e, "skipping an unreadable lease record");
                        continue;
                    }
                };
                match serde_json::from_slice::<Lease>(&bytes) {
                    Ok(lease) => out.push(lease),
                    Err(source) => {
                        return Err(RegistryError::Malformed { node: key, source });
                    }
                }
            }
            Ok(out)
        })
    }

    fn withdraw(&self, node: &NodeId) -> BoxFuture<'_, Result<(), RegistryError>> {
        let key = record_name(node);
        Box::pin(async move {
            // S3 treats deleting an absent key as success, so this is
            // idempotent without a probe — the property `DirRegistry` has to
            // get by matching `NotFound`.
            self.bucket.delete(&key).await.map_err(into_registry_error)
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_bucket_without_a_prefix_is_refused() {
        // The registry inherits the store's rule rather than restating it: a
        // cluster registry that could address a bucket root could delete
        // anything in it, and "delete my key" is the one operation here that
        // is destructive.
        let err = S3Registry::from_uri("my-bucket").await.unwrap_err();
        assert!(err.to_string().contains("names no prefix"), "{err}");
    }

    #[test]
    fn every_s3_failure_reads_as_an_incomplete_round_trip() {
        // The lease loop extends its hold deadline only on a *complete* round
        // trip. If some S3 errors mapped to a shape a caller might treat as
        // benign, a node that can write but not read could keep renewing its
        // right to hold streams it can no longer be told to release — which is
        // precisely the case the two-part argument in `lease` closes.
        for e in [
            AwsError::Config("no credentials".to_owned()),
            AwsError::Rejected {
                key: "node-a.json".to_owned(),
                message: "slow down".to_owned(),
            },
        ] {
            assert!(matches!(into_registry_error(e), RegistryError::Io(_)));
        }
    }
}
