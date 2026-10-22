//! A local personal replacement must not change an unrelated org's wire
//! instance identity. These durable aliases bind the old cloud identity to
//! the peer's isolated retained local instance.

use crate::storage::NamespacedStore;
use crate::sync::log::{LogEntry, LogOp, MutationEnvelope};
use crate::sync::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

mod physical;

pub const NAMESPACE: &str = "sync_scope_aliases";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScopeAlias {
    pub format_version: u32,
    pub target_prefix: String,
    pub wire_prefix: Option<String>,
    pub local_prefix: String,
    pub schema_names: Vec<String>,
}

fn wire(prefix: Option<&str>) -> &str {
    prefix
        .filter(|value| !value.is_empty())
        .unwrap_or(crate::db_operations::UNPREFIXED_INSTANCE_ID)
}

fn replay_key(target: &str, prefix: Option<&str>) -> Vec<u8> {
    format!("replay:{target}:{}", wire(prefix)).into_bytes()
}

fn capture_key(prefix: &str) -> Vec<u8> {
    format!("capture:{prefix}").into_bytes()
}

impl ScopeAlias {
    fn validate(&self) -> SyncResult<()> {
        if self.format_version != 1
            || !crate::hex::is_lower_hex_sha256(&self.target_prefix)
            || !crate::hex::is_lower_hex_sha256(&self.local_prefix)
            || self
                .wire_prefix
                .as_ref()
                .is_some_and(|value| !crate::hex::is_lower_hex_sha256(value))
            || self.schema_names.is_empty()
        {
            return Err(SyncError::CloudPurgeHeld(
                "invalid retained org instance alias".into(),
            ));
        }
        Ok(())
    }
}

/// Resolve target-bound wire instances before any exact atom read or replay.
pub async fn to_local(
    store: &dyn NamespacedStore,
    target: &str,
    mutations: &mut [MutationEnvelope],
) -> SyncResult<()> {
    if target.is_empty() {
        return Ok(());
    }
    let namespace = store.open_namespace(NAMESPACE).await?;
    let keys: BTreeSet<_> = mutations
        .iter()
        .map(|mutation| replay_key(target, mutation.storage_prefix.as_deref()))
        .collect();
    let keys: Vec<_> = keys.into_iter().collect();
    let values = namespace.get_many(keys.clone()).await?;
    let aliases: BTreeMap<_, _> = keys
        .into_iter()
        .zip(values)
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect();
    for mutation in mutations {
        let key = replay_key(target, mutation.storage_prefix.as_deref());
        if let Some(bytes) = aliases.get(&key) {
            let alias: ScopeAlias = serde_json::from_slice(bytes)?;
            alias.validate()?;
            if alias.target_prefix != target || !alias.schema_names.contains(&mutation.schema_name)
            {
                return Err(SyncError::CloudPurgeHeld(
                    "org replay alias scope differs".into(),
                ));
            }
            mutation.storage_prefix = Some(alias.local_prefix);
        }
    }
    Ok(())
}

/// Restore wire identities after local atom materialization. Author clocks,
/// operation ids, writer frontiers, and field values stay unchanged.
pub async fn to_wire(
    store: &dyn NamespacedStore,
    target: &str,
    mutations: &mut [MutationEnvelope],
) -> SyncResult<()> {
    if target.is_empty() {
        return Ok(());
    }
    let namespace = store.open_namespace(NAMESPACE).await?;
    let prefixes: BTreeSet<_> = mutations
        .iter()
        .filter_map(|mutation| mutation.storage_prefix.clone())
        .collect();
    let prefixes: Vec<_> = prefixes.into_iter().collect();
    let values = namespace
        .get_many(prefixes.iter().map(|prefix| capture_key(prefix)).collect())
        .await?;
    let aliases: BTreeMap<_, _> = prefixes
        .into_iter()
        .zip(values)
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect();
    for mutation in mutations {
        if let Some(bytes) = mutation
            .storage_prefix
            .as_ref()
            .and_then(|prefix| aliases.get(prefix))
        {
            let alias: ScopeAlias = serde_json::from_slice(bytes)?;
            alias.validate()?;
            if alias.target_prefix != target || !alias.schema_names.contains(&mutation.schema_name)
            {
                return Err(SyncError::CloudPurgeHeld(
                    "org upload alias scope differs".into(),
                ));
            }
            mutation.storage_prefix = alias.wire_prefix;
        }
    }
    Ok(())
}

/// Inactive queue copies already know the exact old cloud target. Typed
/// intent prefixes can change locally without changing the cloud stream.
pub async fn rebind_entry(
    store: &dyn NamespacedStore,
    target: &str,
    entry: &mut LogEntry,
) -> SyncResult<()> {
    match &mut entry.op {
        LogOp::MutationIntent { mutations } => to_local(store, target, mutations).await,
        op => physical::rewrite(store, target, op, false).await,
    }
}

pub(crate) async fn wire_entry(
    store: &dyn NamespacedStore,
    target: &str,
    entry: &mut LogEntry,
) -> SyncResult<()> {
    match &mut entry.op {
        LogOp::MutationIntent { mutations } => to_wire(store, target, mutations).await,
        op => physical::rewrite(store, target, op, true).await,
    }
}
