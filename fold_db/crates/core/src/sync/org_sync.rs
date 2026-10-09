//! Key-prefix-based sync partitioning for scoped sync data.
//!
//! The `SyncPartitioner` inspects each log entry's key to determine whether it
//! should be synced to the personal S3 prefix or a share prefix.
//!
//! This is much simpler than a mapping table — the key itself tells you where
//! the data belongs. No registration of atoms/molecules required.
//!
//! ## Key format
//!
//! - Personal: `atom:{uuid}`, per-key molecule tips (`mk:`/`mh:`/`tv:`/…),
//!   `history:{mol}:{ts}`. Legacy `ref:{uuid}` whole-molecule blobs may still
//!   appear on the wire from older peers (sync migrates/deletes them); product
//!   create/update never emits new `ref:` keys.
//! - Share: `share:{sender_hash}:{opaque}:atom:{uuid}`, etc.

use crate::crypto::CryptoProvider;
use base64::Engine;
use std::sync::Arc;

/// A sync target — one R2 prefix with its own encryption key.
///
/// Personal sync and scoped sync are the same mechanism: upload/download
/// encrypted log entries to/from `/{prefix}/log/{seq}.enc`.
#[derive(Clone)]
pub struct SyncTarget {
    /// Human-readable label for logging ("personal" or share name).
    pub label: String,
    /// R2 prefix hash: `user_hash` for personal, share prefix for shares.
    pub prefix: String,
    /// Crypto provider for sealing/unsealing entries on this prefix.
    pub crypto: Arc<dyn CryptoProvider>,
}

/// Where a log entry should be synced to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncDestination {
    /// Personal data — sync to `/{user_hash}/log/{seq}.enc`
    Personal,
    /// Shared data — sync to `/{share_prefix}/log/{seq}.enc`
    /// with the share's E2E key for encryption.
    Share {
        share_prefix: String,
        share_e2e_secret: String,
    },
}

/// Partitions log entries by destination based on key prefix.
///
/// Given a LogEntry key, determines whether it belongs to an active share or is
/// personal data.
#[derive(Clone)]
pub struct SyncPartitioner {
    /// Active share targets with their E2E secrets.
    share_targets: Vec<ShareTargetEntry>,
}

#[derive(Debug, Clone)]
struct ShareTargetEntry {
    /// Prefix present in local storage keys.
    storage_prefix: String,
    /// Prefix of the remote cloud head.
    target_prefix: String,
    share_e2e_secret: String,
    /// Org slug when this entry came from an org sync target.
    org_slug: Option<String>,
    /// Schema allowlist for a legacy unprefixed personal instance.
    /// Empty preserves the legacy whole-instance behavior.
    unprefixed_schema_names: Vec<String>,
}

impl SyncPartitioner {
    /// Create a new partitioner from active share targets.
    pub fn new(share_rules: &[crate::sharing::types::ShareRule]) -> Self {
        let share_targets = share_rules
            .iter()
            .filter(|r| r.active)
            .map(|r| ShareTargetEntry {
                storage_prefix: r.share_prefix.clone(),
                target_prefix: r.share_prefix.clone(),
                share_e2e_secret: base64::engine::general_purpose::STANDARD
                    .encode(&r.share_e2e_secret),
                org_slug: None,
                unprefixed_schema_names: Vec::new(),
            })
            .collect();

        Self { share_targets }
    }

    /// Partitioner for share rules **and** registered org cloud-sync targets.
    ///
    /// Org targets explicitly map local database storage prefixes to the org
    /// cloud head. Legacy rows without a mapping still match `org_hash`.
    pub fn new_with_orgs(
        share_rules: &[crate::sharing::types::ShareRule],
        org_targets: &[crate::sharing::OrgSyncTarget],
    ) -> Self {
        let mut share_targets = share_rules
            .iter()
            .filter(|r| r.active)
            .map(|r| ShareTargetEntry {
                storage_prefix: r.share_prefix.clone(),
                target_prefix: r.share_prefix.clone(),
                share_e2e_secret: base64::engine::general_purpose::STANDARD
                    .encode(&r.share_e2e_secret),
                org_slug: None,
                unprefixed_schema_names: Vec::new(),
            })
            .collect::<Vec<_>>();
        for org in org_targets.iter().filter(|t| t.active) {
            let storage_prefixes = if org.storage_prefixes.is_empty() {
                std::slice::from_ref(&org.org_hash)
            } else {
                org.storage_prefixes.as_slice()
            };
            share_targets.extend(
                storage_prefixes
                    .iter()
                    .map(|storage_prefix| ShareTargetEntry {
                        storage_prefix: storage_prefix.clone(),
                        target_prefix: org.org_hash.clone(),
                        share_e2e_secret: org.e2e_key_b64.clone(),
                        org_slug: Some(org.slug.clone()),
                        unprefixed_schema_names: org.unprefixed_schema_names.clone(),
                    }),
            );
        }
        Self { share_targets }
    }

    /// Create an empty partitioner (no shares — everything is personal).
    pub fn empty() -> Self {
        Self {
            share_targets: Vec::new(),
        }
    }

    /// Determine where a key should be synced based on its prefix.
    ///
    /// Matches both:
    /// - `{share_prefix}:…` at byte 0 (plain org / share rows), and
    /// - native-index embedding keys `emb:{share_prefix}:…` /
    ///   `graveyard:emb:{share_prefix}:…` where the storage prefix sits after a
    ///   fixed prefix (same rules as [`storage_prefix_for_key`]).
    ///
    /// Without the native-index arm, active org embeddings classify as
    /// Personal and upload under the wrong seal key / cloud path.
    pub fn partition(&self, key: &str) -> SyncDestination {
        if let Some(dest) = self.match_share_prefix(key) {
            return dest;
        }
        // Embedding / graveyard rows put the org hash *after* a fixed prefix.
        // Without this arm, active org emb keys classify as Personal and
        // upload under the wrong seal key / cloud path.
        for native_prefix in NATIVE_INDEX_KEY_PREFIXES {
            if let Some(rest) = key.strip_prefix(native_prefix) {
                if let Some(dest) = self.match_share_prefix(rest) {
                    return dest;
                }
            }
        }
        SyncDestination::Personal
    }

    fn match_share_prefix(&self, key: &str) -> Option<SyncDestination> {
        for entry in &self.share_targets {
            if entry.storage_prefix == crate::db_operations::UNPREFIXED_INSTANCE_ID {
                if !entry.unprefixed_schema_names.is_empty() {
                    continue;
                }
                if crate::db_operations::leftover_db_hash_prefix(key.as_bytes()).is_none()
                    && key.contains(':')
                {
                    return Some(SyncDestination::Share {
                        share_prefix: entry.target_prefix.clone(),
                        share_e2e_secret: entry.share_e2e_secret.clone(),
                    });
                }
                continue;
            }
            let prefix = format!("{}:", entry.storage_prefix);
            if key.starts_with(&prefix) {
                return Some(SyncDestination::Share {
                    share_prefix: entry.target_prefix.clone(),
                    share_e2e_secret: entry.share_e2e_secret.clone(),
                });
            }
        }
        None
    }

    /// Resolve an exact local database storage prefix to its cloud target.
    pub fn partition_storage_prefix(&self, storage_prefix: &str) -> Option<SyncDestination> {
        self.share_targets
            .iter()
            .find(|entry| entry.storage_prefix == storage_prefix)
            .map(|entry| SyncDestination::Share {
                share_prefix: entry.target_prefix.clone(),
                share_e2e_secret: entry.share_e2e_secret.clone(),
            })
    }

    /// Route an unprefixed MutationIntent only when every mutation belongs to
    /// a schema explicitly shared from the legacy personal instance.
    pub fn partition_mutation_intent(
        &self,
        mutations: &[crate::sync::log::MutationEnvelope],
    ) -> SyncDestination {
        let schemas: Vec<&str> = mutations
            .iter()
            .map(|mutation| mutation.schema_name.as_str())
            .collect();
        self.share_targets
            .iter()
            .find(|target| {
                target.storage_prefix == crate::db_operations::UNPREFIXED_INSTANCE_ID
                    && (target.unprefixed_schema_names.is_empty()
                        || schemas.iter().all(|schema| {
                            target
                                .unprefixed_schema_names
                                .iter()
                                .any(|shared| shared == schema)
                        }))
            })
            .map_or(SyncDestination::Personal, |target| SyncDestination::Share {
                share_prefix: target.target_prefix.clone(),
                share_e2e_secret: target.share_e2e_secret.clone(),
            })
    }

    /// Partition a LogOp's base64-encoded key string.
    ///
    /// On base64 or UTF-8 decode failure, falls back to
    /// [`SyncDestination::Personal`] and logs a `tracing::warn!` so silent
    /// mis-routing is visible in ops without failing the whole partition
    /// pass. Destination rules match [`Self::partition`] (including org
    /// `emb:` / `graveyard:emb:` keys).
    pub fn partition_log_key(&self, key_b64: &str) -> SyncDestination {
        use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
        let Ok(key_bytes) = BASE64.decode(key_b64) else {
            tracing::warn!(
                key_b64_prefix = %truncate_for_log(key_b64, 32),
                "partition_log_key: base64 decode failed; treating as Personal"
            );
            return SyncDestination::Personal;
        };
        let Ok(key_str) = std::str::from_utf8(&key_bytes) else {
            tracing::warn!(
                decoded_len = key_bytes.len(),
                "partition_log_key: key bytes are not UTF-8; treating as Personal"
            );
            return SyncDestination::Personal;
        };

        self.partition(key_str)
    }

    /// Route a log op. `db_catalog` membership uses the locator in the JSON
    /// body so org rows land on the org head (the key is `v1:{sha256}` with
    /// no storage prefix). Other namespaces keep key-prefix routing.
    pub fn partition_catalog_or_key(
        &self,
        namespace: &str,
        key_b64: &str,
        value_b64: Option<&str>,
    ) -> SyncDestination {
        if namespace != "db_catalog" {
            return self.partition_log_key(key_b64);
        }
        let Some(value_b64) = value_b64 else {
            return SyncDestination::Personal;
        };
        use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
        let Ok(bytes) = BASE64.decode(value_b64) else {
            return SyncDestination::Personal;
        };
        let Ok(entry) = serde_json::from_slice::<crate::db_operations::DbCatalogEntry>(&bytes)
        else {
            return SyncDestination::Personal;
        };
        let Ok(parsed) = crate::access::parse_db_locator(&entry.db_locator) else {
            return SyncDestination::Personal;
        };
        match parsed {
            crate::access::DbLocator::Personal => SyncDestination::Personal,
            crate::access::DbLocator::Org { ref org_slug, .. } => self
                .share_targets
                .iter()
                .find(|target| target.org_slug.as_deref() == Some(org_slug.as_str()))
                .map(|target| SyncDestination::Share {
                    share_prefix: target.target_prefix.clone(),
                    share_e2e_secret: target.share_e2e_secret.clone(),
                })
                .or_else(|| {
                    crate::access::storage_prefix_for(&parsed)
                        .and_then(|prefix| self.partition_storage_prefix(&prefix))
                })
                .unwrap_or(SyncDestination::Personal),
            crate::access::DbLocator::DbHash(hash) => self
                .partition_storage_prefix(&hash)
                .unwrap_or(SyncDestination::Personal),
        }
    }
}

/// Cap noisy key material in warn lines (base64 key strings can be long).
fn truncate_for_log(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars).collect();
    out.push('…');
    out
}

/// Strip the org prefix from a key, if present.
///
/// Returns `Some((storage_prefix, base_key))` if the key has an org prefix,
/// or `None` if it's a personal key.
pub fn strip_storage_prefix(key: &str) -> Option<(&str, &str)> {
    // Org keys look like: {storage_prefix}:{rest}, where storage_prefix is a
    // 64-char hex SHA256.
    crate::kind_partition::split_org_storage_prefix(key)
}

/// Native-index storage-key prefixes that carry the storage_prefix *after* the
/// prefix rather than at the start of the key.
///
/// Atom / ref / metadata rows are keyed `{storage_prefix}:atom:…`, so the storage_prefix
/// is at byte 0 and [`strip_storage_prefix`] finds it directly. Embedding rows
/// are keyed `emb:{storage_prefix}:{schema}:…` (the `emb:` / `graveyard:emb:`
/// prefix comes first), so a naive `strip_storage_prefix` on the full key misses
/// the storage_prefix and the personal crypto provider gets selected — which then
/// fails to decrypt an org-E2E-encrypted value. See
/// [`storage_prefix_for_key`].
const NATIVE_INDEX_KEY_PREFIXES: &[&str] = &["emb:", "graveyard:emb:"];

/// Resolve the storage_prefix that owns a storage key, if any.
///
/// Unlike [`strip_storage_prefix`] (which only matches a storage_prefix at byte 0),
/// this also recognizes the native-index embedding-key shape
/// `emb:{storage_prefix}:…` / `graveyard:emb:{storage_prefix}:…`, where a fixed prefix
/// precedes the storage_prefix. This is the form crypto-provider selection must
/// understand: an org member who synced org-scoped embeddings stores them
/// encrypted under the org E2E key, and the restore/scan path must pick the
/// org crypto provider for those rows, not the personal one.
///
/// Returns the storage_prefix slice when the key is org-scoped, else `None`.
pub fn storage_prefix_for_key(key: &str) -> Option<&str> {
    if let Some((storage_prefix, _)) = strip_storage_prefix(key) {
        return Some(storage_prefix);
    }
    for prefix in NATIVE_INDEX_KEY_PREFIXES {
        if let Some(rest) = key.strip_prefix(prefix) {
            if let Some((storage_prefix, _)) = strip_storage_prefix(rest) {
                return Some(storage_prefix);
            }
        }
    }
    None
}
