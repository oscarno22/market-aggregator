//! S3, behind the `s3` feature.
//!
//! # The gate this module sits behind, and why it is structural
//!
//! CLAUDE.md's sequencing rule is explicit: *nothing writes to S3 before an
//! IAM user scoped to one bucket prefix replaces the root keys.* That is not a
//! note in a runbook here — it is enforced in three places, because a rule
//! that depends on remembering it is a rule that gets forgotten at 2am:
//!
//! 1. **The feature is off by default.** A default build does not compile or
//!    link the AWS SDK, so the offline suite cannot acquire a dependency on
//!    credentials by accident, and `just test` cannot reach AWS even in
//!    principle.
//! 2. **A prefix is mandatory.** [`S3Store::from_uri`] refuses a bare
//!    `s3://bucket`. A process that can write to the root of a bucket can
//!    overwrite anything in it, and "scoped to one bucket prefix" is only true
//!    if the writer actually confines itself to one.
//! 3. **Reaching S3 has to be asserted, not defaulted into.**
//!    [`S3Store::connect`] refuses to start unless `MA_S3_ACK_SCOPED_IAM=1`.
//!
//! Point 3 deserves to be described accurately rather than flatteringly: this
//! code **cannot tell a scoped IAM user's credentials from a root user's**.
//! Both are `AKIA…` access keys and nothing available to the process
//! distinguishes them. So the interlock does not verify the scoping — it
//! requires an operator to state that they have done it. That is a weak
//! control and is not pretending to be more.
//!
//! It is still worth having, because the failure the rule in CLAUDE.md is
//! actually about is not a malicious operator. It is a long-running ingest
//! process quietly inheriting whatever credentials were in the environment,
//! and nobody noticing until something is overwritten. An interlock turns that
//! from a default into a decision.
//!
//! None of this makes the IAM policy correct — only an IAM policy does that.
//!
//! # Status
//!
//! Written, compiled under `--features s3`, and **not yet exercised against a
//! live bucket** — see `docs/DESIGN.md` §9. The `ObjectStore` contract it
//! implements is proven by `LocalStore` and by the round-trip tests; what is
//! unproven is this file's behaviour against real S3 semantics, which no
//! amount of local testing can establish.

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use tracing::info;

use crate::store::{ObjectStore, StoreError};

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// An S3 bucket, confined to one key prefix.
#[derive(Clone, Debug)]
pub struct S3Store {
    client: Client,
    bucket: String,
    /// Never empty. See the module docs: a store that can write to a bucket
    /// root is not "scoped to one prefix" whatever the IAM policy says.
    prefix: String,
}

impl S3Store {
    /// Split `bucket/prefix` (the part after `s3://`).
    ///
    /// # Errors
    /// If the bucket or the prefix is missing. The prefix is not optional, and
    /// the error says why rather than defaulting to the bucket root.
    pub fn parse_uri(rest: &str) -> Result<(String, String), StoreError> {
        let (bucket, prefix) = rest.split_once('/').ok_or_else(|| {
            StoreError::Config(format!(
                "s3://{rest} names no prefix. A prefix is required: this process \
                 should be scoped to one, and a writer that can reach a bucket \
                 root can overwrite everything in it. Use s3://{rest}/<prefix>."
            ))
        })?;
        let prefix = prefix.trim_matches('/');
        if bucket.is_empty() || prefix.is_empty() {
            return Err(StoreError::Config(format!(
                "s3://{rest} is missing a bucket or a prefix"
            )));
        }
        Ok((bucket.to_owned(), prefix.to_owned()))
    }

    /// Parse and connect in one step. `rest` is the part after `s3://`.
    ///
    /// # Errors
    /// As [`Self::parse_uri`] and [`Self::connect`].
    pub async fn from_uri(rest: &str) -> Result<Self, StoreError> {
        let (bucket, prefix) = Self::parse_uri(rest)?;
        Self::connect(&bucket, &prefix).await
    }

    /// Build a client from the ambient AWS configuration.
    ///
    /// # The interlock, and what it does not do
    ///
    /// Nothing here can distinguish a scoped IAM user's credentials from the
    /// root account's — both arrive as `AKIA…` access keys and the SDK offers
    /// no way to ask. So this does not *verify* CLAUDE.md's scoping rule; it
    /// requires the operator to assert it, via `MA_S3_ACK_SCOPED_IAM=1`.
    ///
    /// The value of that is narrow and real. The failure worth preventing is a
    /// long-running ingest process silently inheriting whatever credentials
    /// happened to be in its environment. An interlock makes reaching S3 a
    /// decision somebody made rather than a default that happened.
    ///
    /// # Errors
    /// If the scoping acknowledgement is not set, or the prefix is empty.
    pub async fn connect(bucket: &str, prefix: &str) -> Result<Self, StoreError> {
        check_interlock(std::env::var(ACK_VAR).ok().as_deref())?;

        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        let prefix = prefix.trim_matches('/').to_owned();
        if prefix.is_empty() {
            return Err(StoreError::Config(
                "an S3 store must be scoped to a non-empty prefix".to_owned(),
            ));
        }

        info!(bucket, prefix, "s3 store configured");
        Ok(Self {
            client,
            bucket: bucket.to_owned(),
            prefix,
        })
    }

    fn key(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key.trim_start_matches('/'))
    }
}

/// The variable an operator sets to assert they have done the IAM scoping.
pub const ACK_VAR: &str = "MA_S3_ACK_SCOPED_IAM";

/// The interlock, as a pure function of what the environment said.
///
/// Split out so it is testable. The workspace forbids `unsafe`, and mutating
/// process environment in a test requires it — which is a good constraint
/// rather than an obstacle: a check that reads ambient state directly is one
/// that can only be tested by mutating global state, and that is a design smell
/// wherever it appears, not just here.
fn check_interlock(ack: Option<&str>) -> Result<(), StoreError> {
    if ack == Some("1") {
        return Ok(());
    }
    Err(StoreError::Config(format!(
        "refusing to write to S3 without {ACK_VAR}=1. Set it only once an IAM \
         user scoped to this one bucket prefix has replaced any root \
         credentials — see CLAUDE.md's sequencing rule and docs/DESIGN.md §9. \
         Note that this process cannot verify the scoping; setting the \
         variable asserts it."
    )))
}

impl ObjectStore for S3Store {
    fn describe(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    fn put(&self, key: &str, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), StoreError>> {
        let full = self.key(key);
        let key = key.to_owned();
        Box::pin(async move {
            // No staging-then-rename equivalent is needed, and none is
            // attempted: S3 `PutObject` is atomic for a single object. A reader
            // sees the previous version or the new one, never a partial write —
            // which is exactly the property `LocalStore` has to construct by
            // hand because POSIX does not offer it.
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&full)
                .body(ByteStream::from(bytes))
                .send()
                .await
                .map_err(|e| StoreError::Rejected {
                    key,
                    message: e.to_string(),
                })?;
            Ok(())
        })
    }

    fn get(&self, key: &str) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        let full = self.key(key);
        let key = key.to_owned();
        Box::pin(async move {
            let response = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&full)
                .send()
                .await
                .map_err(|e| StoreError::Rejected {
                    key: key.clone(),
                    message: e.to_string(),
                })?;
            let bytes = response
                .body
                .collect()
                .await
                .map_err(|e| StoreError::Rejected {
                    key,
                    message: e.to_string(),
                })?;
            Ok(bytes.into_bytes().to_vec())
        })
    }

    fn list(&self, prefix: &str) -> BoxFuture<'_, Result<Vec<String>, StoreError>> {
        let full = self.key(prefix);
        let strip = format!("{}/", self.prefix);
        let reported = prefix.to_owned();
        Box::pin(async move {
            let mut out = Vec::new();
            // Paginated, because a bucket holding a month of hourly files has
            // more than one page and a truncated listing would replay a partial
            // session without saying so.
            let mut pages = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full)
                .into_paginator()
                .send();

            while let Some(page) = pages.next().await {
                let page = page.map_err(|e| StoreError::Rejected {
                    key: reported.clone(),
                    message: e.to_string(),
                })?;
                for object in page.contents() {
                    if let Some(key) = object.key()
                        && let Some(rel) = key.strip_prefix(&strip)
                    {
                        out.push(rel.to_owned());
                    }
                }
            }
            // S3 returns keys in UTF-8 binary order already, but sorting is
            // cheap and the reader's chronological guarantee should not depend
            // on a remote service's ordering promise.
            out.sort();
            Ok(out)
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_without_a_prefix_is_refused() {
        // "Scoped to one bucket prefix" is only true if the writer confines
        // itself to one. A store that can address the bucket root can overwrite
        // everything in it, whatever the IAM policy happens to say today.
        let err = S3Store::parse_uri("my-bucket").unwrap_err();
        assert!(err.to_string().contains("names no prefix"), "{err}");

        let err = S3Store::parse_uri("my-bucket/").unwrap_err();
        assert!(
            err.to_string().contains("missing a bucket or a prefix"),
            "{err}"
        );

        assert_eq!(
            S3Store::parse_uri("my-bucket/market-data/events").unwrap(),
            ("my-bucket".to_owned(), "market-data/events".to_owned())
        );
    }

    #[test]
    fn s3_is_refused_until_the_operator_asserts_the_iam_scoping() {
        // The interlock is weak on purpose and does not pretend otherwise: it
        // cannot tell a scoped key from a root one. Its job is to stop a
        // long-running process from silently inheriting whatever credentials
        // were in its environment.
        for absent in [None, Some(""), Some("0"), Some("yes"), Some("true")] {
            let err = check_interlock(absent).unwrap_err();
            assert!(
                err.to_string().contains(ACK_VAR),
                "{absent:?} did not trip the interlock: {err}"
            );
        }
        assert!(check_interlock(Some("1")).is_ok());
    }

    #[test]
    fn the_refusal_admits_what_it_cannot_check() {
        // An interlock that implied it had verified the IAM scoping would be
        // worse than none: it would license exactly the confidence it cannot
        // support.
        let message = check_interlock(None).unwrap_err().to_string();
        assert!(
            message.contains("cannot verify"),
            "the error overstates what the check proves: {message}"
        );
    }
}
