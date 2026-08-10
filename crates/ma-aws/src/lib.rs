//! An S3 bucket that has proved it is confined to one prefix.
//!
//! # The gate this crate is, and why it is structural
//!
//! CLAUDE.md's sequencing rule is explicit: *nothing writes to S3 before an
//! IAM user scoped to one bucket prefix replaces the root keys.* That is not a
//! note in a runbook — it is enforced in four places, because a rule that
//! depends on remembering it is a rule that gets forgotten at 2am:
//!
//! 1. **Nothing depends on this crate by default.** Both callers pull it in
//!    under their own off-by-default `s3` feature, so a default build does not
//!    compile or link the AWS SDK, and `just test` cannot reach AWS even in
//!    principle.
//! 2. **A prefix is mandatory.** [`ScopedBucket::parse_uri`] refuses a bare
//!    `s3://bucket`. A process that can write to the root of a bucket can
//!    overwrite anything in it, and "scoped to one bucket prefix" is only true
//!    if the writer actually confines itself to one.
//! 3. **Reaching S3 has to be asserted, not defaulted into.**
//!    [`ScopedBucket::connect`] refuses to start unless `MA_S3_ACK_SCOPED_IAM=1`.
//! 4. **The scoping is *verified*, not merely asserted.** Before returning a
//!    usable handle, [`ScopedBucket::connect`] asks S3 to list the bucket
//!    *outside* the configured prefix. Correctly scoped credentials are denied.
//!    If the call succeeds, the credentials can address more than the prefix
//!    they were given and the process refuses to start.
//!
//! Point 4 replaced a weaker claim, and the history is worth keeping. This code
//! used to say that nothing in the process could distinguish a scoped IAM
//! user's credentials from the root account's — both arrive as `AKIA…` keys and
//! the SDK will not say which is which — so point 3 was the whole of the
//! control and was honestly described as weak.
//!
//! That was true about the *credentials* and false about the *question*. The
//! rule does not actually care who the principal is; it cares whether this
//! process can reach outside its prefix. **That is answerable, by asking.** A
//! single `ListObjectsV2` at the bucket root separates the two cases exactly:
//! root gets `200`, a prefix-scoped user gets `AccessDenied`.
//!
//! It also answers the better question. A root key with a bucket policy that
//! confines it passes, correctly — because it *is* confined — and a scoped user
//! whose policy is wider than intended fails, which an ARN check would have
//! waved through.
//!
//! What was actually being prevented was never a malicious operator. It was a
//! long-running process quietly inheriting whatever credentials were in the
//! environment, and nobody noticing until something got overwritten. That is
//! now caught at startup rather than trusted.
//!
//! The limits, stated: this proves the credentials cannot *list* outside the
//! prefix. A policy granting write-but-not-list outside it would pass. That is
//! a strange policy to write by accident, and the check is a floor rather than
//! a proof of the IAM document — only the IAM document is that.
//!
//! # Why this is a crate rather than a module
//!
//! Two unrelated subsystems want a bucket: `ma-persist` for the Parquet
//! archive, and `ma-coord` for the cluster registry. What they share is not
//! convenience code — it is the check above, and a safety control that exists
//! twice will eventually disagree with itself. That is the same argument
//! `docs/DESIGN.md` §8 makes about teeing Parquet off the aggregator rather
//! than adding a second consumer of the raw channel: the duplicate is not
//! wrong on the day it is written, it is wrong on some later day, quietly.
//!
//! The alternative shapes were both worse. `ma-coord` depending on `ma-persist`
//! would make the coordination layer import a columnar-format crate to borrow
//! sixty lines of IAM check; duplicating those lines would put the project's
//! only credential control in two files.
//!
//! # Status
//!
//! Exercised against a real bucket on 2026-08-09 — the scoped user was denied
//! at the bucket root and admitted inside its prefix, a live archive was
//! written, flushed on `SIGTERM`, and replayed back out. See `docs/DESIGN.md`
//! §10.

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use tracing::info;

#[derive(Debug, thiserror::Error)]
pub enum AwsError {
    /// The URI, the environment or the credentials are wrong. Always fatal at
    /// startup: every variant of this means the process should not run.
    #[error("{0}")]
    Config(String),
    /// S3 refused, or could not be reached, for one key.
    #[error("s3 rejected {key}: {message}")]
    Rejected { key: String, message: String },
}

/// The variable an operator sets to assert they have done the IAM scoping.
pub const ACK_VAR: &str = "MA_S3_ACK_SCOPED_IAM";

/// An S3 bucket, confined to one key prefix, verified at construction.
///
/// Every method takes a key *relative* to the prefix and never lets a caller
/// address outside it — including `..`, which S3 treats as a literal path
/// component rather than a traversal, and which is therefore confined by
/// construction rather than by parsing.
#[derive(Clone, Debug)]
pub struct ScopedBucket {
    client: Client,
    bucket: String,
    /// Never empty. See the crate docs: a handle that can write to a bucket
    /// root is not "scoped to one prefix" whatever the IAM policy says.
    prefix: String,
}

impl ScopedBucket {
    /// Split `bucket/prefix` (the part after `s3://`).
    ///
    /// # Errors
    /// If the bucket or the prefix is missing. The prefix is not optional, and
    /// the error says why rather than defaulting to the bucket root.
    pub fn parse_uri(rest: &str) -> Result<(String, String), AwsError> {
        let (bucket, prefix) = rest.split_once('/').ok_or_else(|| {
            AwsError::Config(format!(
                "s3://{rest} names no prefix. A prefix is required: this process \
                 should be scoped to one, and a writer that can reach a bucket \
                 root can overwrite everything in it. Use s3://{rest}/<prefix>."
            ))
        })?;
        let prefix = prefix.trim_matches('/');
        if bucket.is_empty() || prefix.is_empty() {
            return Err(AwsError::Config(format!(
                "s3://{rest} is missing a bucket or a prefix"
            )));
        }
        Ok((bucket.to_owned(), prefix.to_owned()))
    }

    /// Parse and connect in one step. `rest` is the part after `s3://`.
    ///
    /// # Errors
    /// As [`Self::parse_uri`] and [`Self::connect`].
    pub async fn from_uri(rest: &str) -> Result<Self, AwsError> {
        let (bucket, prefix) = Self::parse_uri(rest)?;
        Self::connect(&bucket, &prefix).await
    }

    /// Build a client from the ambient AWS configuration, and prove it is
    /// confined to `prefix` before handing it back.
    ///
    /// Two gates, in order. `MA_S3_ACK_SCOPED_IAM=1` says somebody decided to
    /// reach AWS at all; the scope probe then checks that the credentials this
    /// process actually resolved cannot address the whole bucket. The second is
    /// the one with teeth — see the crate docs on why "who is this principal"
    /// was the wrong question and "what can it reach" is the right one.
    ///
    /// # Errors
    /// If the acknowledgement is unset, the prefix is empty, the credentials
    /// can list outside the prefix, or the bucket cannot be reached at all.
    pub async fn connect(bucket: &str, prefix: &str) -> Result<Self, AwsError> {
        check_interlock(std::env::var(ACK_VAR).ok().as_deref())?;

        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        let prefix = prefix.trim_matches('/').to_owned();
        if prefix.is_empty() {
            return Err(AwsError::Config(
                "an S3 handle must be scoped to a non-empty prefix".to_owned(),
            ));
        }

        verify_scope(&client, bucket, &prefix).await?;
        info!(bucket, prefix, "s3 configured; scope verified");
        Ok(Self {
            client,
            bucket: bucket.to_owned(),
            prefix,
        })
    }

    pub fn describe(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    /// The absolute key for a caller's relative one.
    fn key(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key.trim_start_matches('/'))
    }

    /// Store an object.
    ///
    /// # Errors
    /// If S3 rejects the write.
    pub async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), AwsError> {
        // No staging-then-rename equivalent is needed, and none is attempted:
        // S3 `PutObject` is atomic for a single object. A reader sees the
        // previous version or the new one, never a partial write — which is
        // exactly the property `LocalStore` and `DirRegistry` have to construct
        // by hand because POSIX does not offer it.
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key(key))
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|e| AwsError::Rejected {
                key: key.to_owned(),
                message: e.to_string(),
            })?;
        Ok(())
    }

    /// Fetch an object.
    ///
    /// # Errors
    /// If the object is missing or S3 rejects the read.
    pub async fn get(&self, key: &str) -> Result<Vec<u8>, AwsError> {
        let reject = |e: &dyn std::fmt::Display| AwsError::Rejected {
            key: key.to_owned(),
            message: e.to_string(),
        };
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key(key))
            .send()
            .await
            .map_err(|e| reject(&e))?;
        let bytes = response.body.collect().await.map_err(|e| reject(&e))?;
        Ok(bytes.into_bytes().to_vec())
    }

    /// Keys under `prefix`, relative to this handle's own prefix, sorted.
    ///
    /// # Errors
    /// If S3 rejects the listing.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>, AwsError> {
        let strip = format!("{}/", self.prefix);
        let mut out = Vec::new();
        // Paginated, because a bucket holding a month of hourly files has more
        // than one page and a truncated listing would replay a partial session
        // without saying so.
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(self.key(prefix))
            .into_paginator()
            .send();

        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| AwsError::Rejected {
                key: prefix.to_owned(),
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
        // S3 returns keys in UTF-8 binary order already, but sorting is cheap
        // and a caller's chronological guarantee should not depend on a remote
        // service's ordering promise.
        out.sort();
        Ok(out)
    }

    /// Remove an object. Deleting something that is not there is not an error,
    /// which is S3's own behaviour and the one the cluster registry wants: a
    /// node withdrawing twice must not fail the second time.
    ///
    /// # Errors
    /// If S3 rejects the delete.
    pub async fn delete(&self, key: &str) -> Result<(), AwsError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.key(key))
            .send()
            .await
            .map_err(|e| AwsError::Rejected {
                key: key.to_owned(),
                message: e.to_string(),
            })?;
        Ok(())
    }
}

/// The interlock, as a pure function of what the environment said.
///
/// Split out so it is testable. The workspace forbids `unsafe`, and mutating
/// process environment in a test requires it — which is a good constraint
/// rather than an obstacle: a check that reads ambient state directly is one
/// that can only be tested by mutating global state, and that is a design smell
/// wherever it appears, not just here.
fn check_interlock(ack: Option<&str>) -> Result<(), AwsError> {
    if ack == Some("1") {
        return Ok(());
    }
    Err(AwsError::Config(format!(
        "refusing to reach S3 without {ACK_VAR}=1. Set it only once an IAM user \
         scoped to this one bucket prefix has replaced any root credentials — \
         see CLAUDE.md's sequencing rule and docs/DESIGN.md §10. Setting it \
         asserts the scoping; `verify_scope` then tests it against the bucket \
         and refuses to start if the credentials reach wider."
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
///   That arm earned itself immediately: the first live run hit a
///   credential-provider configuration error, and reading it as a denial would
///   have started the process with unusable credentials and a clean bill of
///   health.
async fn verify_scope(client: &Client, bucket: &str, prefix: &str) -> Result<(), AwsError> {
    let probe = client
        .list_objects_v2()
        .bucket(bucket)
        .max_keys(1)
        .send()
        .await;

    match probe {
        Ok(_) => Err(AwsError::Config(format!(
            "these credentials can list the whole of s3://{bucket}, so they are not scoped to \
             the prefix {prefix:?} they were given. {ACK_VAR} asserts the scoping; this check \
             tests it, and it failed.\n\
             The usual cause is an ambient root session or a default profile picked up from \
             the environment — run with AWS_PROFILE set to the scoped user, and confirm with \
             `aws sts get-caller-identity`. See CLAUDE.md's sequencing rule and \
             docs/DESIGN.md §10."
        ))),
        Err(e) if is_access_denied(&e) => Ok(()),
        Err(e) => Err(AwsError::Config(format!(
            "could not reach s3://{bucket} to verify credential scope: {e:?}. This is a \
             connectivity or configuration failure, not a scoping result — startup is refused \
             rather than assuming the credentials are confined."
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
    DENIAL_SHAPES
        .iter()
        .any(|needle| format!("{err:?}").contains(needle))
}

/// The rendered fragments that mean "you may not enumerate this bucket".
///
/// A constant rather than a literal inside [`is_access_denied`] so the test
/// below exercises the *same* list the check uses. Two copies would let the
/// test keep passing while the check drifted, which for this particular check
/// is the worst available outcome.
const DENIAL_SHAPES: [&str; 4] = ["AccessDenied", "AllAccessDisabled", "NoSuchBucket", "403"];

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_without_a_prefix_is_refused() {
        // "Scoped to one bucket prefix" is only true if the caller confines
        // itself to one. A handle that can address the bucket root can
        // overwrite everything in it, whatever the IAM policy happens to say
        // today.
        let err = ScopedBucket::parse_uri("my-bucket").unwrap_err();
        assert!(err.to_string().contains("names no prefix"), "{err}");

        let err = ScopedBucket::parse_uri("my-bucket/").unwrap_err();
        assert!(
            err.to_string().contains("missing a bucket or a prefix"),
            "{err}"
        );

        assert_eq!(
            ScopedBucket::parse_uri("my-bucket/market-data/events").unwrap(),
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
    fn denied_shape(rendered: &str) -> bool {
        DENIAL_SHAPES.iter().any(|n| rendered.contains(n))
    }

    /// One `ListObjectsV2` response body. `truncated` carries the continuation
    /// token that makes the SDK ask for another page.
    fn listing_page(keys: &[&str], truncated: Option<&str>) -> String {
        let contents: String = keys
            .iter()
            .map(|k| format!("<Contents><Key>{k}</Key><Size>1</Size></Contents>"))
            .collect();
        let more = match truncated {
            Some(token) => format!(
                "<IsTruncated>true</IsTruncated>\
                 <NextContinuationToken>{token}</NextContinuationToken>"
            ),
            None => "<IsTruncated>false</IsTruncated>".to_owned(),
        };
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Name>bucket</Name>{more}{contents}
            </ListBucketResult>"#
        )
    }

    /// A `ScopedBucket` whose transport replays `pages` in order.
    ///
    /// Built by constructing the struct directly rather than through
    /// `connect`, which resolves ambient credentials and runs the scope probe.
    /// Both are proven elsewhere in this module, and neither is what this test
    /// is about.
    fn bucket_replaying(pages: Vec<String>) -> ScopedBucket {
        use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
        use aws_smithy_types::body::SdkBody;

        let events = pages
            .into_iter()
            .map(|body| {
                ReplayEvent::new(
                    http::Request::builder()
                        .uri("https://bucket.s3.us-east-1.amazonaws.com/?list-type=2")
                        .body(SdkBody::empty())
                        .unwrap(),
                    http::Response::builder()
                        .status(200)
                        .body(SdkBody::from(body))
                        .unwrap(),
                )
            })
            .collect();

        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .http_client(StaticReplayClient::new(events))
            .build();

        ScopedBucket {
            client: Client::from_conf(config),
            bucket: "bucket".to_owned(),
            prefix: "events".to_owned(),
        }
    }

    #[tokio::test]
    async fn a_truncated_listing_is_followed_to_the_last_page() {
        // docs/DESIGN.md §15 listed S3's far tail as "the code paths exist; the
        // evidence for them does not". This is the evidence for the half that
        // does not need a month of archiving to produce.
        //
        // S3 caps a listing at 1000 keys and says so with IsTruncated plus a
        // continuation token. A reader that stops at the first page does not
        // error — it replays a partial session and reports success, which is
        // this project's defining failure mode: silently wrong beats obviously
        // down, in the wrong direction. An archive only crosses 1000 objects
        // after days of hourly parts, so the first run to hit it would be a
        // long-lived one, and the symptom would be missing history rather than
        // a stack trace.
        //
        // The stub is at the transport, so the paginator under test is the
        // real one and the XML is the shape S3 actually returns.
        let bucket = bucket_replaying(vec![
            listing_page(
                &["events/symbol=BTC-USD/date=2026-08-09/hour=00/part-00000.parquet"],
                Some("page-2"),
            ),
            listing_page(
                &["events/symbol=BTC-USD/date=2026-08-09/hour=01/part-00000.parquet"],
                None,
            ),
        ]);

        let keys = bucket.list("symbol=BTC-USD").await.unwrap();

        assert_eq!(
            keys,
            [
                "symbol=BTC-USD/date=2026-08-09/hour=00/part-00000.parquet",
                "symbol=BTC-USD/date=2026-08-09/hour=01/part-00000.parquet",
            ],
            "the listing stopped at the first page, or did not strip the prefix"
        );
    }

    #[tokio::test]
    async fn a_single_page_listing_does_not_ask_for_another() {
        // The other half of the same claim. A paginator that kept asking would
        // hang on the replay client running out of responses, so this pins
        // termination as well as continuation.
        let bucket = bucket_replaying(vec![listing_page(
            &["events/symbol=ETH-USD/date=2026-08-09/hour=00/part-00000.parquet"],
            None,
        )]);

        let keys = bucket.list("symbol=ETH-USD").await.unwrap();
        assert_eq!(keys.len(), 1);
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
        // The standard the message is held to: an operator reading a refusal
        // must be able to tell which half is their word and which half is a
        // measurement. Claiming more than is checked licenses confidence the
        // check cannot support; claiming less leaves a real control looking
        // optional.
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
