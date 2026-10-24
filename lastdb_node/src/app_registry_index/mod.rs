//! The static signed app index — the anonymous read surface of the LastDB app
//! registry (brain `decision-2026-09-19-app-registry-is-the-release-unit`).
//!
//! One JSON file per channel (`stable`, `next`) plus a detached Ed25519
//! signature. Each app row carries **compat rows**: `<app_version> @ <sha>`
//! proved with `lastdb <version>` by one named proof run. Install reads the
//! running node's build string and picks the newest row that names it. There
//! is no minimum-version rule: a minimum alone fails when the node removes
//! something, so only an exact proved pair counts.
//!
//! The index is served as a plain file (the Homebrew tap repo, or any static
//! host). A fresh install needs no account and no live service. Publish stays
//! DevCert-gated elsewhere; this module only signs, verifies, resolves, and
//! installs.
//!
//! Trust: the verifying key is pinned in this binary
//! ([`RELEASE_INDEX_PUBKEY_B64`]). An operator can override it for a test
//! index with `--trust-key` or `LASTDB_REGISTRY_TRUST_KEY` (a base64 key or a
//! path to a file holding one); the override is printed, never silent.

use std::time::Duration;

mod install;
mod location;
mod node_version;
mod resolve;
mod signing;
mod version_constraint;

pub use install::{
    install_pinned, upgrade_pinned, PinnedInstallOutcome, PinnedUpgradeOutcome, RECEIPT_FILE,
};
pub use location::{validate_channel, IndexLocation};
pub use node_version::running_lastdb_version;
pub use resolve::{fetch_verified, pick_row, resolve, Resolution};
pub use signing::{
    load_verified_index, parse_index, resolve_trust_key, sign_index_bytes, verify_index_bytes,
    CompatRow, IndexApp, IndexSignature, RegistryIndex, TrustSource,
};
pub use version_constraint::{check_constraint, check_manifest_constraint, Constraint, Version};

/// The index grammar this binary reads and writes.
pub const INDEX_VERSION: u32 = 1;

/// The channel a public install reads when none is named.
pub const DEFAULT_CHANNEL: &str = "stable";

/// Where the public index lives: the Homebrew tap repo, served raw by GitHub.
/// `<base>/<channel>.json` and `<base>/<channel>.json.sig`.
pub const DEFAULT_INDEX_BASE_URL: &str =
    "https://raw.githubusercontent.com/EdgeVector/homebrew-lastdb/main/registry";

/// The Ed25519 verifying key that signs the public index. Generated
/// 2026-09-20 (`lastdb app dev-init --key-file ~/.lastdb/registry-index-signing.key`
/// on the release host); the same key is published as
/// `registry/index-signing.pub` in the tap repo so a reader can cross-check.
pub const RELEASE_INDEX_PUBKEY_B64: &str = "Aw807ErGvi7+cJ0RhSvRbAt9UWwKOwrfAPvAYF4fNdQ=";

/// Environment override for the index location (a URL base or a directory).
pub const INDEX_LOCATION_ENV: &str = "LASTDB_REGISTRY_INDEX";
/// Environment override for the trusted verifying key.
pub const TRUST_KEY_ENV: &str = "LASTDB_REGISTRY_TRUST_KEY";

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
