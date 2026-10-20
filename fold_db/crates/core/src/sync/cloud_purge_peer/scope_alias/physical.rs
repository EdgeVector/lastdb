//! Historical physical queue keys retain their unchanged org wire prefix.

use super::*;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};

fn fields(op: &mut LogOp) -> Vec<(&str, &mut String)> {
    match op {
        LogOp::Put { namespace, key, .. } | LogOp::Delete { namespace, key } => {
            vec![(namespace, key)]
        }
        LogOp::BatchPut { namespace, items } | LogOp::PhysicalDigest { namespace, items } => items
            .iter_mut()
            .map(|(key, _)| (namespace.as_str(), key))
            .collect(),
        LogOp::BatchDelete { namespace, keys } => keys
            .iter_mut()
            .map(|key| (namespace.as_str(), key))
            .collect(),
        LogOp::LogicalCommit { changes } => changes
            .iter_mut()
            .map(|change| (change.namespace.as_str(), &mut change.key))
            .collect(),
        _ => Vec::new(),
    }
}

fn data_namespace(namespace: &str) -> bool {
    matches!(
        namespace,
        "main"
            | "atoms"
            | "tips"
            | "indexes"
            | "proteins"
            | "mutation_history"
            | "field_hashrange_page_index"
            | "field_hashrange_hash_index"
            | "field_hashrange_complete"
    )
}

pub(super) async fn rewrite(
    store: &dyn NamespacedStore,
    target: &str,
    op: &mut LogOp,
    outbound: bool,
) -> SyncResult<()> {
    if target.is_empty() {
        return Ok(());
    }
    let mut decoded = Vec::new();
    for (namespace, key) in fields(op) {
        if !data_namespace(namespace) {
            continue;
        }
        let bytes = BASE64.decode(key.as_bytes()).map_err(|_| {
            SyncError::CloudPurgeHeld("retained org physical key is invalid".into())
        })?;
        let text = String::from_utf8(bytes).map_err(|_| {
            SyncError::CloudPurgeHeld("retained org physical key is not text".into())
        })?;
        let (prefix, bare) = crate::sync::org_sync::strip_storage_prefix(&text)
            .map_or((None, text.as_str()), |(prefix, bare)| (Some(prefix), bare));
        let lookup = if outbound {
            let Some(prefix) = prefix else {
                continue;
            };
            capture_key(prefix)
        } else {
            replay_key(target, prefix)
        };
        decoded.push((key, lookup, bare.to_string()));
    }
    let namespace = store.open_namespace(NAMESPACE).await?;
    let keys: Vec<_> = decoded
        .iter()
        .map(|(_, key, _)| key.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let values = namespace.get_many(keys.clone()).await?;
    let aliases: BTreeMap<_, _> = keys
        .into_iter()
        .zip(values)
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect();
    for (key, lookup, bare) in decoded {
        if let Some(bytes) = aliases.get(&lookup) {
            let alias: ScopeAlias = serde_json::from_slice(bytes)?;
            alias.validate()?;
            if alias.target_prefix != target {
                return Err(SyncError::CloudPurgeHeld(
                    "physical org alias account differs".into(),
                ));
            }
            let prefix = if outbound {
                alias.wire_prefix.as_deref()
            } else {
                Some(alias.local_prefix.as_str())
            };
            *key = BASE64.encode(prefix.map_or(bare.clone(), |prefix| format!("{prefix}:{bare}")));
        }
    }
    Ok(())
}
