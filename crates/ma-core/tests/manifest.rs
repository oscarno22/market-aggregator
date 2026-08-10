//! Guards two claims made outside the code, by reading the manifests that
//! would falsify them.
//!
//! The first is that book logic is testable without a network:
//! `docs/DESIGN.md` asserts that everything in `ma-core` runs offline and
//! deterministically. That claim is only worth making if something enforces it,
//! and a code review two months from now is not something.
//!
//! The second is the assumption `.cargo/audit.toml` rests on — see the test
//! itself. It lives here rather than in a workspace-level suite because this
//! workspace has no root package, so there is nowhere above `crates/` for a
//! test to run, and this is already the file that reads manifests.

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

/// Every manifest in the workspace that depends on `rust_decimal`, paired with
/// the crate name for the failure message. `include_str!` rather than a
/// directory walk, so this stays a compile-time read and `ma-core` keeps its
/// no-I/O property.
const RUST_DECIMAL_MANIFESTS: &[(&str, &str)] = &[
    ("ma-core", include_str!("../Cargo.toml")),
    ("ma-venues", include_str!("../../ma-venues/Cargo.toml")),
    ("ma-server", include_str!("../../ma-server/Cargo.toml")),
];

/// Pins the one fact `.cargo/audit.toml`'s RUSTSEC-2026-0235 exception rests
/// on: that `rust_decimal`'s optional `rkyv` feature is never enabled, so rkyv
/// 0.7.46 is in `Cargo.lock` but never compiled.
///
/// The advisory cannot be resolved by upgrading — rust_decimal's latest release
/// still declares rkyv 0.7.46, and the 0.7 series is unsupported upstream — so
/// the exception is permanent until rust_decimal moves. That makes the
/// assumption underneath it worth enforcing rather than commenting.
///
/// This is deliberately a *blocking* test rather than part of the advisory
/// audit job. Enabling a feature is a change we make; an advisory is one that
/// lands on us. Only the first should be able to fail a build.
#[test]
fn rust_decimal_never_enables_rkyv() {
    let mut offenders = Vec::new();

    for (crate_name, manifest) in RUST_DECIMAL_MANIFESTS {
        for line in manifest.lines() {
            let line = line.trim();

            // Comments here and in the manifests discuss rkyv by name while
            // explaining its absence, so match on declarations only.
            if line.starts_with('#') || !line.starts_with("rust_decimal") {
                continue;
            }

            if line.contains("rkyv") {
                offenders.push(format!("{crate_name} enables rkyv: {line}"));
            }

            // Without this, a future `default-features = true` would pull in
            // whatever rust_decimal defaults to and the line above would not
            // notice, because the feature name never appears in our manifest.
            if !line.contains("default-features = false") {
                offenders.push(format!(
                    "{crate_name} takes rust_decimal's default features: {line}"
                ));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "rust_decimal is now pulling rkyv into the build:\n  {}\n\n\
         RUSTSEC-2026-0235 (rkyv < 0.8.17, out-of-bounds reads via Rc/Arc in \
         archives) is ignored in .cargo/audit.toml *because* rkyv is never \
         compiled. That is no longer true, so the exception is no longer sound: \
         either revert the feature change, or remove the ignore and deal with \
         the advisory.",
        offenders.join("\n  ")
    );
}
