//! Guards the claim that book logic is testable without a network.
//!
//! `docs/DESIGN.md` asserts that everything in `ma-core` runs offline and
//! deterministically. That claim is only worth making if something enforces it,
//! and a code review two months from now is not something.

/// Crates that would drag in a runtime, a socket, or a system clock we cannot
/// control. Any of these appearing in `ma-core` means the layering has been
/// breached and the offline guarantee is no longer true.
const FORBIDDEN: &[&str] = &[
    "tokio",
    "tokio-tungstenite",
    "async-std",
    "smol",
    "reqwest",
    "hyper",
    "axum",
    "futures",
    "futures-util",
    "arrow",
    "parquet",
    "aws-sdk-s3",
];

#[test]
fn ma_core_has_no_async_or_io_dependencies() {
    let manifest = include_str!("../Cargo.toml");

    let mut in_deps = false;
    let mut offenders = Vec::new();

    for line in manifest.lines() {
        let line = line.trim();

        // Comments mention several of these crates by name while explaining
        // why they are absent, so skip them rather than matching on them.
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        if line.starts_with('[') {
            in_deps = matches!(
                line,
                "[dependencies]" | "[dev-dependencies]" | "[build-dependencies]"
            );
            continue;
        }

        if !in_deps {
            continue;
        }

        let Some((name, _)) = line.split_once('=') else {
            continue;
        };
        let name = name.trim().trim_matches('"');

        if FORBIDDEN.contains(&name) {
            offenders.push(name.to_owned());
        }
    }

    assert!(
        offenders.is_empty(),
        "ma-core gained async/IO dependencies: {offenders:?}.\n\
         This breaks the guarantee that book and sync logic are unit-testable \
         with no network. If the dependency is genuinely needed, it belongs in \
         ma-pipeline, and docs/DESIGN.md needs updating to stop claiming otherwise."
    );
}
