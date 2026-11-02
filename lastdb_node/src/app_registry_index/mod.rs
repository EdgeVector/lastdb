//! Anonymous installation from a KMS-signed S3 list for one exact API version.
//! Every entry also carries a publisher Ed25519 signature over RFC 8785 fields.
//! Verified cached lists survive failed downloads and reject sequence rollback.
//! Legacy index signing helpers remain for historical GitHub files.

use std::time::Duration;

mod install;
mod interface_list;
mod list_cache;
mod location;
mod node_version;
mod resolve;
mod resolve_local;
mod signing;
mod version_constraint;

pub use install::{
    install_pinned, upgrade_pinned, PinnedInstallOutcome, PinnedUpgradeOutcome, RECEIPT_FILE,
};
pub use interface_list::{valid_app_id, valid_source, InterfaceList, ListEntry};
pub use list_cache::fetch_list;
pub use location::{validate_channel, IndexLocation};
pub use node_version::{running_lastdb_version, running_node};
pub use resolve::{fetch_verified, pick_row, resolve, ContractVersions, NodeVersion, Resolution};
pub use resolve_local::resolve_local;
pub use signing::{
    load_verified_index, parse_index, resolve_list_trust_key, resolve_trust_key, sign_index_bytes,
    verify_index_bytes, CompatRow, IndexApp, IndexSignature, RegistryIndex, TrustSource,
};
pub use version_constraint::{check_constraint, check_manifest_constraint, Constraint, Version};

/// The index grammar this binary reads and writes.
pub const INDEX_VERSION: u32 = 1;

/// The channel a public install reads when none is named.
pub const DEFAULT_CHANNEL: &str = "stable";

/// Public list base. Fetch `<base>/<channel>/api/<api_version>.json` and `.sig`.
pub const DEFAULT_INDEX_BASE_URL: &str =
    "https://lastdb-app-registry-prod-152335099025.s3.us-east-1.amazonaws.com/v1/lists";

/// Registry KMS Ed25519 public key. The private half never leaves AWS KMS.
pub const REGISTRY_LIST_PUBKEY_B64: &str = "3xySKyIR6nD2GjBXq15Jp6UtkasK2+OAhCUEvBd60sw=";

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
