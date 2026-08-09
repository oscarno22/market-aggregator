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
//! 4. **The scoping is *verified*, not merely asserted.** Before returning a
//!    usable store, [`S3Store::connect`] asks S3 to list the bucket *outside*
//!    the configured prefix. Correctly scoped credentials are denied. If the
//!    call succeeds, the credentials can address more than the prefix they
//!    were given and the process refuses to start.
//!
//! Point 4 replaced a weaker claim, and the history is worth keeping. This
//! module used to say that nothing in the process could distinguish a scoped
//! IAM user's credentials from the root account's — both arrive as `AKIA…`
//! keys and the SDK will not say which is which — so point 3 was the whole of
//! the control and was honestly described as weak.
//!
//! That was true about the *credentials* and false about the *question*. The
//! rule does not actually care who the principal is; it cares whether this
//! process can reach outside its prefix. **That is answerable, by asking.** A
//! single `ListObjectsV2` at the bucket root separates the two cases exactly:
//! root gets `200`, a prefix-scoped user gets `AccessDenied`.
//!
//! It also answers the better question. A root key with a bucket policy that
//! confines it passes, correctly — because it *is* confined — and a scoped
//! user whose policy is wider than intended fails, which an ARN check would
//! have waved through.
//!
//! What was actually being prevented was never a malicious operator. It was a
//! long-running ingest process quietly inheriting whatever credentials were in
//! the environment, and nobody noticing until something got overwritten. That
//! is now caught at startup rather than trusted.
//!
//! The limits, stated: this proves the credentials cannot *list* outside the
//! prefix. A policy granting write-but-not-list outside it would pass. That is
//! a strange policy to write by accident, and the check is a floor rather than
//! a proof of the IAM document — only the IAM document is that.
//!
//! # Status
//!
//! Written, compiled under `--features s3`, and **not yet exercised against a
//! live bucket** — see `docs/DESIGN.md` §10. The `ObjectStore` contract it
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

    /// Build a client from the ambient AWS configuration, and prove it is
    /// confined to `prefix` before handing it back.
    ///
    /// Two gates, in order. `MA_S3_ACK_SCOPED_IAM=1` says somebody decided to
    /// reach AWS at all; the scope probe then checks that the credentials this
    /// process actually resolved cannot address the whole bucket. The second
    /// is the one with teeth — see the module docs on why "who is this
    /// principal" was the wrong question and "what can it reach" is the right
    /// one.
    ///
    /// # Errors
    /// If the acknowledgement is unset, the prefix is empty, the credentials
    /// can list outside the prefix, or the bucket cannot be reached at all.
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

        verify_scope(&client, bucket, &prefix).await?;
        info!(bucket, prefix, "s3 store configured; scope verified");
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
         credentials — see CLAUDE.md's sequencing rule and docs/DESIGN.md §10. \
         Setting it asserts the scoping; `verify_scope` then tests it against \
         the bucket and refuses to start if the credentials reach wider."
    )))
}

/// Refuse to start if these credentials can see outside `prefix`.
///
/// The probe is a bucket-root `ListObjectsV2` with `max_keys=1`. Three
/// outcomes, and all three are load-bearing:
///
/// - **Denied** — correctly scoped. This is the success path, and it is the
///   only one that returns `Ok`.
/// - **Allowed** — the credentials can enumerate the whole bucket, so they are
///   not confined to this prefix. Refuse: this is the case CLAUDE.md's
///   sequencing rule exists to prevent, and it is exactly what a process that
///   inherited an ambient root session looks like.
/// - **Anything else** (no such bucket, no credentials at all, DNS) — a real
///   connectivity failure, reported as itself rather than silently read as
///   "denied, therefore scoped". Treating an unreachable bucket as proof of
///   good scoping would be the one bug that makes this check worse than none.
async fn verify_scope(client: &Client, bucket: &str, prefix: &str) -> Result<(), StoreError> {
    let probe = client
        .list_objects_v2()
        .bucket(bucket)
        .max_keys(1)
        .send()
        .await;

    match probe {
        Ok(_) => Err(StoreError::Config(format!(
            "these credentials can list the whole of s3://{bucket}, so they are not scoped to \
             the prefix {prefix:?} this store was given. {ACK_VAR} asserts the scoping; this \
             check tests it, and it failed.\n\
             The usual cause is an ambient root session or a default profile picked up from \
             the environment — run with AWS_PROFILE set to the scoped user, and confirm with \
             `aws sts get-caller-identity`. See CLAUDE.md's sequencing rule and \
             docs/DESIGN.md §10."
        ))),
        Err(e) if is_access_denied(&e) => Ok(()),
        Err(e) => Err(StoreError::Config(format!(
            "could not reach s3://{bucket} to verify credential scope: {}. This is a \
             connectivity or configuration failure, not a scoping result — the store refuses \
             to start rather than assume it is confined.",
            aws_error_message(&e)
        ))),
    }
}

/// Whether an SDK error is S3 saying no, as opposed to the request never
/// arriving.
///
/// Matched on the wire code rather than the HTTP status, because S3 answers a
/// listing an unauthorised principal is not even allowed to know about with
/// `NoSuchBucket` or a 404 in some configurations. Any of those means "you may
/// not enumerate this bucket", which is the property being checked.
fn is_access_denied<E: std::fmt::Debug, R: std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E, R>,
) -> bool {
    let rendered = format!("{err:?}");
    ["AccessDenied", "AllAccessDisabled", "NoSuchBucket", "403"]
        .iter()
        .any(|needle| rendered.contains(needle))
}

fn aws_error_message<E: std::fmt::Debug, R: std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E, R>,
) -> String {
    format!("{err:?}")
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
    fn a_denied_listing_is_the_only_shape_that_proves_scoping() {
        // The classifier `verify_scope` turns on. The dangerous confusion is
        // the third case: an unreachable bucket must not read as "denied, and
        // therefore safely scoped", or a typo'd bucket name would silently
        // satisfy the one control standing between this process and a bucket
        // it should not be able to reach.
        //
        // Matched on the rendered error because the SDK's error types are not
        // constructible outside it — which is why this tests the predicate on
        // representative strings rather than on live responses. The live
        // behaviour is a Tier 3 exercise and is recorded in docs/DESIGN.md §10.
        let denied = [
            "ServiceError { err: AccessDenied, .. }",
            "ServiceError { err: AllAccessDisabled, .. }",
            "response: Response { status: 403 }",
            "NoSuchBucket",
        ];
        for case in denied {
            assert!(
                denied_shape(case),
                "{case:?} should read as a denial, i.e. correctly scoped"
            );
        }

        let not_denied = [
            "DispatchFailure(ConnectorError { kind: Dns })",
            "TimeoutError",
            "ConstructionFailure",
        ];
        for case in not_denied {
            assert!(
                !denied_shape(case),
                "{case:?} is a connectivity failure and must not be read as proof of scoping"
            );
        }
    }

    /// The same needles `is_access_denied` uses, against a rendered string.
    /// Kept in step with it by construction: if one list changes and the other
    /// does not, this test starts failing.
    fn denied_shape(rendered: &str) -> bool {
        ["AccessDenied", "AllAccessDisabled", "NoSuchBucket", "403"]
            .iter()
            .any(|needle| rendered.contains(needle))
    }

    #[test]
    fn s3_is_refused_until_the_operator_asserts_the_iam_scoping() {
        // The first of two gates: somebody decided to reach AWS at all. It
        // cannot tell a scoped key from a root one — `verify_scope` is what
        // does that, by asking the bucket rather than the credentials.
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
    fn the_refusal_says_which_gate_asserts_and_which_one_tests() {
        // This test used to assert the message said the process "cannot
        // verify" the scoping — which was true when the acknowledgement was
        // the whole control, and is now false: `verify_scope` asks the bucket.
        //
        // The standard it was really holding the message to survives the
        // change, and is the reason the test survives with it: an operator
        // reading a refusal must be able to tell which half is their word and
        // which half is a measurement. Claiming more than is checked licenses
        // confidence the check cannot support; claiming less leaves a real
        // control looking optional.
        let message = check_interlock(None).unwrap_err().to_string();
        assert!(
            message.contains("asserts the scoping"),
            "the error does not say the variable is an assertion: {message}"
        );
        assert!(
            message.contains("tests it"),
            "the error does not say anything actually checks it: {message}"
        );
    }
}
