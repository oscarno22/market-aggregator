# The IAM policy that gates every S3 run

The `market-aggregator` IAM user is the only credential the Rust binaries
ever use (`AWS_PROFILE=market-aggregator`; the ambient default is typically a
root login session, which `ma-aws` deliberately cannot consume — its
`aws-config` build excludes `credentials-login`). This document exists
because the policy's *shape* is load-bearing in a way a policy JSON cannot
say about itself, and because for a while the only copy of it lived in a
console nobody could diff against.

> **Status: behaviourally verified, textually reconstructed.** The scoped
> user cannot read its own policy (`iam:ListUserPolicies` → AccessDenied,
> which is correct), so the JSON below is reconstructed rather than exported.
> Every operative claim in it was verified against the live bucket on
> 2026-08-09: bucket-root listing **denied** (the probe's success condition),
> `cluster/` list/put/delete **allowed** (a full round trip), `events/`
> sub-prefix listing **allowed** (the part-number resume ran against it and
> produced `part-00001` beside `part-00000`), and the S3 registry's
> `withdraw` **succeeded** where the pre-widening policy refused it. If the
> console text ever drifts from this file, those four behaviours are the
> diff that matters.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ObjectsInScopedPrefixes",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::market-aggregator-176331939239-us-east-1/events/*",
        "arn:aws:s3:::market-aggregator-176331939239-us-east-1/cluster/*"
      ]
    },
    {
      "Sid": "ListOnlyInsideScopedPrefixes",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::market-aggregator-176331939239-us-east-1",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["events/*", "cluster/*"]
        }
      }
    }
  ]
}
```

## The `s3:prefix` condition is load-bearing — do not "fix" it

The condition on `ListBucket` is not least-privilege hygiene. It is what
makes the startup scope probe **pass**.

`ScopedBucket::connect` (in `ma-aws`) proves it is running under a scoped
credential by listing the bucket root with **no prefix** and accepting only
*denial* as success. The condition above leaves that request unmatched and
therefore denied — which is the one outcome the probe reads as "this
credential is scoped". Root is allowed to list the root, so root fails the
probe and is refused. Any other error is *also* a refusal, never read as
proof of scoping.

Granting `s3:ListBucket` unconditionally would make the probe's root listing
succeed — and **every S3 run in the project would refuse to start**. If that
ever happens, the diagnosis is this paragraph.

## What each grant is for

| Grant | Who needs it |
|---|---|
| `PutObject` on `events/*` | The Parquet writer's uploads |
| `GetObject` + `ListBucket` under `events/*` | Archive replay, and the writer's part-number resume — on first touch of an hour directory it lists what a previous run left there, so a same-hour restart appends `part-N+1` instead of overwriting `part-00000` |
| `Put/Get/List` under `cluster/*` | The lease registry: each node writes only its own key, reads everyone's |
| `DeleteObject` on `cluster/*` | A clean shutdown's `withdraw`, so the survivor rebalances immediately instead of waiting out the lease. Absent from the original policy — see `DESIGN.md` §13 for what that refusal proved |
| `DeleteObject` on `events/*` | Nothing in the pipeline deletes archive objects; granted for operator cleanup only |

Deliberately **no conditional writes and no compare-and-swap anywhere**: the
registry needs none because no node ever writes a key another node writes.
That is why a plain object store is a complete registry — `DESIGN.md` §13.

## The one failure mode this setup leaves

A run that forgets `AWS_PROFILE` resolves the ambient default, which is
typically an expired `aws login` session. It fails **closed**: the scope
probe's error arm treats a credential failure as "not proof of scoping" and
refuses to start. Confusing message, safe outcome.
