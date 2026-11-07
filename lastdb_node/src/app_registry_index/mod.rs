//! Anonymous installation from a KMS-signed S3 list for one exact API version.
//! Every entry also carries a publisher Ed25519 signature over RFC 8785 fields.
//! Verified cached lists survive failed downloads and reject sequence rollback.
//! Legacy index signing helpers remain for historical GitHub files.

use std::time::Duration;

mod install;
mod list_cache;
mod location;
pub mod managed;
mod managed_links;
mod managed_manifest;
pub mod managed_poll;
mod managed_process;
mod managed_publisher;
mod node_version;
mod resolve;
mod signing;

pub use app_identity_crypto::{
    sign_index_bytes, valid_app_id, valid_source, validate_channel, verify_index_bytes,
    IndexSignature, InterfaceList, ListEntry, REGISTRY_LIST_PUBKEY_B64,
};
pub use install::{install_pinned, PinnedInstallOutcome, RECEIPT_FILE};
pub use list_cache::fetch_list;
pub use location::IndexLocation;
pub use node_version::running_node;
pub use resolve::{resolve, ContractVersions, NodeVersion, Resolution};
pub use signing::{
    load_verified_index, parse_index, resolve_list_trust_key, resolve_trust_key, CompatRow,
    IndexApp, RegistryIndex, TrustSource,
};

/// The index grammar this binary reads and writes.
pub const INDEX_VERSION: u32 = 1;

/// The channel a public install reads when none is named.
pub const DEFAULT_CHANNEL: &str = "stable";

/// Public list base. Fetch `<base>/<channel>/api/<api_version>.json` and `.sig`.
pub const DEFAULT_INDEX_BASE_URL: &str =
    folddb_profile::endpoints::registry_index_url_for(folddb_profile::endpoints::Environment::Prod);

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
