//! Org cloud-sync target registry.
//!
//! An org database is a normal LastDB database that always has a cloud sync
//! backup. Registration stores the org's cloud prefix (`org_hash`, 64-char hex)
//! and the shared E2E key so the sync engine can append/download the org log.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::error::FoldDbError;
use crate::storage::KvStore;

const ORG_SYNC_TREE: &str = "org_sync_targets";

/// Registered org cloud-sync target (local node).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrgSyncTarget {
    /// 64-char hex org identity (sha256 of org SPKI) — also the cloud storage prefix.
    pub org_hash: String,
    /// Local database storage prefixes routed to this org cloud head.
    ///
    /// These are derived from registered `X-LastDB-Db` locators. Older rows
    /// omit the field and retain the legacy `{org_hash}:*` routing behavior.
    #[serde(default)]
    pub storage_prefixes: Vec<String>,
    /// Schemas allowed through the legacy unprefixed personal instance.
    ///
    /// An empty list preserves legacy rows that attach the whole unprefixed
    /// instance. New personal-schema shares record the schema explicitly.
    #[serde(default)]
    pub unprefixed_schema_names: Vec<String>,
    /// Base64-encoded 32-byte AES-256 org E2E key.
    pub e2e_key_b64: String,
    /// Optional human slug for status/logging (not used for crypto).
    #[serde(default)]
    pub slug: String,
    pub active: bool,
    /// RFC 3339.
    pub registered_at: String,
}

async fn namespace(ops: &DbOperations) -> Result<Arc<dyn KvStore>, FoldDbError> {
    ops.open_namespace(ORG_SYNC_TREE)
        .await
        .map_err(FoldDbError::from)
}

fn validate_org_hash(org_hash: &str) -> Result<(), FoldDbError> {
    let t = org_hash.trim();
    if t.len() != 64 || !t.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(FoldDbError::Config(format!(
            "org_hash must be 64 hex chars (got len={})",
            t.len()
        )));
    }
    Ok(())
}

fn validate_storage_prefix(storage_prefix: &str) -> Result<(), FoldDbError> {
    let t = storage_prefix.trim();
    if t.eq_ignore_ascii_case(crate::db_operations::UNPREFIXED_INSTANCE_ID) {
        return Ok(());
    }
    validate_org_hash(t)
}

fn validate_e2e_key_b64(e2e_key_b64: &str) -> Result<[u8; 32], FoldDbError> {
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(e2e_key_b64.trim())
        .map_err(|e| FoldDbError::Config(format!("invalid e2e_key_b64: {e}")))?;
    if raw.len() != 32 {
        return Err(FoldDbError::Config(format!(
            "org e2e key must be 32 bytes (got {})",
            raw.len()
        )));
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&raw);
    Ok(key)
}

/// Insert or update an active org sync target in the primary backend.
pub async fn upsert_org_sync_target_in_ops(
    ops: &DbOperations,
    org_hash: &str,
    e2e_key_b64: &str,
    slug: &str,
) -> Result<OrgSyncTarget, FoldDbError> {
    validate_org_hash(org_hash)?;
    let _ = validate_e2e_key_b64(e2e_key_b64)?;
    let target = OrgSyncTarget {
        org_hash: org_hash.trim().to_lowercase(),
        storage_prefixes: Vec::new(),
        unprefixed_schema_names: Vec::new(),
        e2e_key_b64: e2e_key_b64.trim().to_string(),
        slug: slug.trim().to_string(),
        active: true,
        registered_at: chrono_like_now(),
    };
    let store = namespace(ops).await?;
    let key = format!("org_sync:{}", target.org_hash);
    let value = serde_json::to_vec(&target)?;
    store.put(key.as_bytes(), value).await?;
    Ok(target)
}

/// Insert or update an org target and explicitly attach one local database.
///
/// The local storage prefix and the remote org cloud prefix are different
/// identities. Persisting the mapping here lets partitioning route by the
/// former while encryption/upload address the latter.
pub async fn upsert_org_sync_target_for_storage_prefix_in_ops(
    ops: &DbOperations,
    org_hash: &str,
    storage_prefix: &str,
    e2e_key_b64: &str,
    slug: &str,
) -> Result<OrgSyncTarget, FoldDbError> {
    validate_org_hash(org_hash)?;
    validate_storage_prefix(storage_prefix)?;
    let _ = validate_e2e_key_b64(e2e_key_b64)?;

    let org_hash = org_hash.trim().to_lowercase();
    let storage_prefix = storage_prefix.trim().to_lowercase();
    let store = namespace(ops).await?;
    let key = format!("org_sync:{org_hash}");
    let mut target = match store.get(key.as_bytes()).await? {
        Some(value) if !value.is_empty() => serde_json::from_slice::<OrgSyncTarget>(&value)?,
        _ => OrgSyncTarget {
            org_hash: org_hash.clone(),
            storage_prefixes: Vec::new(),
            unprefixed_schema_names: Vec::new(),
            e2e_key_b64: e2e_key_b64.trim().to_string(),
            slug: slug.trim().to_string(),
            active: true,
            registered_at: chrono_like_now(),
        },
    };
    target.e2e_key_b64 = e2e_key_b64.trim().to_string();
    target.slug = slug.trim().to_string();
    target.active = true;
    if !target.storage_prefixes.contains(&storage_prefix) {
        target.storage_prefixes.push(storage_prefix);
        target.storage_prefixes.sort();
    }
    store
        .put(key.as_bytes(), serde_json::to_vec(&target)?)
        .await?;
    Ok(target)
}

/// Insert or update an org target and attach one schema from the legacy
/// unprefixed personal instance.
pub async fn upsert_org_sync_target_for_storage_prefix_and_schema_in_ops(
    ops: &DbOperations,
    org_hash: &str,
    storage_prefix: &str,
    schema_name: &str,
    e2e_key_b64: &str,
    slug: &str,
) -> Result<OrgSyncTarget, FoldDbError> {
    validate_org_hash(org_hash)?;
    validate_storage_prefix(storage_prefix)?;
    if schema_name.trim().is_empty() {
        return Err(FoldDbError::Config(
            "shared schema name must not be empty".to_string(),
        ));
    }
    let _ = validate_e2e_key_b64(e2e_key_b64)?;

    let org_hash = org_hash.trim().to_lowercase();
    let storage_prefix = storage_prefix.trim().to_lowercase();
    let schema_name = schema_name.trim().to_string();
    let store = namespace(ops).await?;
    let key = format!("org_sync:{org_hash}");
    let mut target = match store.get(key.as_bytes()).await? {
        Some(value) if !value.is_empty() => serde_json::from_slice::<OrgSyncTarget>(&value)?,
        _ => OrgSyncTarget {
            org_hash: org_hash.clone(),
            storage_prefixes: Vec::new(),
            unprefixed_schema_names: Vec::new(),
            e2e_key_b64: e2e_key_b64.trim().to_string(),
            slug: slug.trim().to_string(),
            active: true,
            registered_at: chrono_like_now(),
        },
    };
    let had_unprefixed = target
        .storage_prefixes
        .iter()
        .any(|prefix| prefix == crate::db_operations::UNPREFIXED_INSTANCE_ID);
    target.e2e_key_b64 = e2e_key_b64.trim().to_string();
    target.slug = slug.trim().to_string();
    target.active = true;
    if !target.storage_prefixes.contains(&storage_prefix) {
        target.storage_prefixes.push(storage_prefix.clone());
        target.storage_prefixes.sort();
    }
    if storage_prefix == crate::db_operations::UNPREFIXED_INSTANCE_ID
        && (!had_unprefixed || !target.unprefixed_schema_names.is_empty())
        && !target.unprefixed_schema_names.contains(&schema_name)
    {
        target.unprefixed_schema_names.push(schema_name);
        target.unprefixed_schema_names.sort();
    }
    store
        .put(key.as_bytes(), serde_json::to_vec(&target)?)
        .await?;
    Ok(target)
}

/// List all registered org sync targets (active and inactive) from the primary backend.
pub async fn list_org_sync_targets_in_ops(
    ops: &DbOperations,
) -> Result<Vec<OrgSyncTarget>, FoldDbError> {
    let store = namespace(ops).await?;
    let mut items: Vec<OrgSyncTarget> = Vec::new();
    let mut skipped = 0u64;
    for (key, value) in store.scan_prefix(b"").await? {
        if value.is_empty() {
            skipped += 1;
            tracing::warn!(
                key = %String::from_utf8_lossy(&key),
                "org_sync_targets: skipping empty value"
            );
            continue;
        }
        match serde_json::from_slice::<OrgSyncTarget>(&value) {
            Ok(t) => items.push(t),
            Err(e) => {
                skipped += 1;
                tracing::warn!(
                    key = %String::from_utf8_lossy(&key),
                    error = %e,
                    "org_sync_targets: skipping corrupt value"
                );
            }
        }
    }
    if skipped > 0 {
        tracing::warn!(
            skipped,
            kept = items.len(),
            "org_sync_targets: skipped corrupt/empty rows"
        );
    }
    items.sort_by(|a, b| a.org_hash.cmp(&b.org_hash));
    Ok(items)
}

/// Delete empty/corrupt `org_sync_targets` rows from the primary backend.
pub async fn repair_org_sync_targets_in_ops(
    ops: &DbOperations,
    deactivate_all_active: bool,
) -> Result<(u64, u64), FoldDbError> {
    let store = namespace(ops).await?;
    let mut removed_bad = 0u64;
    let mut deactivated = 0u64;
    let keys: Vec<Vec<u8>> = store.scan_prefix_keys(b"").await?.into_iter().collect();
    for key in keys {
        let Some(value) = store.get(&key).await? else {
            continue;
        };
        if value.is_empty() || serde_json::from_slice::<OrgSyncTarget>(&value).is_err() {
            store.delete(&key).await?;
            removed_bad += 1;
            continue;
        }
        if deactivate_all_active {
            let mut target: OrgSyncTarget = serde_json::from_slice(&value)?;
            if target.active {
                target.active = false;
                let encoded = serde_json::to_vec(&target)?;
                store.put(&key, encoded).await?;
                deactivated += 1;
            }
        }
    }
    store.flush().await?;
    Ok((removed_bad, deactivated))
}

/// Active targets only from the primary backend.
pub async fn list_active_org_sync_targets_in_ops(
    ops: &DbOperations,
) -> Result<Vec<OrgSyncTarget>, FoldDbError> {
    Ok(list_org_sync_targets_in_ops(ops)
        .await?
        .into_iter()
        .filter(|t| t.active)
        .collect())
}

/// Deactivate an org sync target in the primary backend.
pub async fn deactivate_org_sync_target_in_ops(
    ops: &DbOperations,
    org_hash: &str,
) -> Result<(), FoldDbError> {
    validate_org_hash(org_hash)?;
    let store = namespace(ops).await?;
    let key = format!("org_sync:{}", org_hash.trim().to_lowercase());
    if let Some(value) = store.get(key.as_bytes()).await? {
        let mut target: OrgSyncTarget = serde_json::from_slice(&value)?;
        target.active = false;
        let value = serde_json::to_vec(&target)?;
        store.put(key.as_bytes(), value).await?;
        return Ok(());
    }
    Err(FoldDbError::Database(format!(
        "org sync target not found: {org_hash}"
    )))
}

/// Deactivate every ACTIVE org sync target that matches the selector.
///
/// The selector is an exact `org_hash`, a `slug`, or both (AND). At least one
/// is required, so a bare call can never disarm every org. A deactivated row
/// keeps its key, prefixes, and slug; `POST /api/org/sync/register` for the
/// same org re-arms it. This touches only the local registry: it makes no
/// cloud call and never claims or releases an Exemem registry row.
///
/// `dry_run` returns the rows that would change and writes nothing.
pub async fn deactivate_org_sync_targets_matching_in_ops(
    ops: &DbOperations,
    org_hash: Option<&str>,
    slug: Option<&str>,
    dry_run: bool,
) -> Result<Vec<OrgSyncTarget>, FoldDbError> {
    let org_hash = org_hash
        .map(|h| h.trim().to_lowercase())
        .filter(|h| !h.is_empty());
    let slug = slug.map(str::trim).filter(|s| !s.is_empty());
    if org_hash.is_none() && slug.is_none() {
        return Err(FoldDbError::Config(
            "org sync deactivate needs org_hash or slug".to_string(),
        ));
    }
    if let Some(h) = org_hash.as_deref() {
        validate_org_hash(h)?;
    }
    let matched: Vec<OrgSyncTarget> = list_org_sync_targets_in_ops(ops)
        .await?
        .into_iter()
        .filter(|t| t.active)
        .filter(|t| org_hash.as_deref().is_none_or(|h| t.org_hash == h))
        .filter(|t| slug.is_none_or(|s| t.slug == s))
        .collect();
    if dry_run || matched.is_empty() {
        return Ok(matched);
    }
    let store = namespace(ops).await?;
    for target in &matched {
        let mut row = target.clone();
        row.active = false;
        let key = format!("org_sync:{}", row.org_hash);
        store.put(key.as_bytes(), serde_json::to_vec(&row)?).await?;
    }
    store.flush().await?;
    Ok(matched)
}

/// Decode the 32-byte E2E key from a registered target.
pub fn e2e_key_bytes(target: &OrgSyncTarget) -> Result<[u8; 32], FoldDbError> {
    validate_e2e_key_b64(&target.e2e_key_b64)
}

fn chrono_like_now() -> String {
    let secs = crate::clock::unix_secs();
    format!("{secs}")
}

// Back-compat aliases used by some call sites that still import the old names.
// These are the same as the `_in_ops` variants.
pub use deactivate_org_sync_target_in_ops as deactivate_org_sync_target;
pub use list_active_org_sync_targets_in_ops as list_active_org_sync_targets;
pub use list_org_sync_targets_in_ops as list_org_sync_targets;
pub use repair_org_sync_targets_in_ops as repair_org_sync_targets;
pub use upsert_org_sync_target_for_storage_prefix_and_schema_in_ops as upsert_org_sync_target_for_storage_prefix_and_schema;
pub use upsert_org_sync_target_for_storage_prefix_in_ops as upsert_org_sync_target_for_storage_prefix;
pub use upsert_org_sync_target_in_ops as upsert_org_sync_target;
