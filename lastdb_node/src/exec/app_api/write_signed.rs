//! After FoldDB commits an owned `/v1/write`, mirror it onto the signed log.

use std::collections::HashMap;

use fold_db::schema::types::{KeyValue, MutationType};
use serde_json::Value;

use crate::exec::reply::HostError;
use crate::host::Host;
use crate::log_sync::{mirror_owned_write, OwnedMutation};

/// One write item's fields needed to author a signed `data` entry.
pub(super) type SignedParts<'a> = (
    &'a str,
    &'a KeyValue,
    MutationType,
    Option<&'a HashMap<String, Value>>,
);

/// Capture schema/key/fields for the signed-history mirror after FoldDB commits.
pub(super) fn plan<'a>(items: impl IntoIterator<Item = SignedParts<'a>>) -> Vec<OwnedMutation> {
    items
        .into_iter()
        .filter_map(|(schema, key, op, record)| {
            let record_key =
                mutation_log::join_record_key(key.hash.as_deref()?, key.range.as_deref())?;
            let fields = record.map(|map| {
                Value::Object(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            });
            Some(OwnedMutation {
                schema: schema.to_owned(),
                record_key,
                op,
                fields,
            })
        })
        .collect()
}

/// The `(schema, record key)` of each mutation, as the history names records.
pub(super) fn keys_of(signed: &[OwnedMutation]) -> Vec<(String, String)> {
    signed
        .iter()
        .map(|item| (item.schema.clone(), item.record_key.clone()))
        .collect()
}

/// Author signed `data` for a shareable owned write; no-op when no database id.
///
/// The store validated and committed the write first. When its entries cannot
/// all be signed, the unsigned records are reverted to the history, so the
/// store never holds a record the history does not.
pub(super) async fn mirror_committed(
    host: &Host,
    database_id: Option<&str>,
    signed: &[OwnedMutation],
) -> Result<(), HostError> {
    let Some(database_id) = database_id else {
        return Ok(());
    };
    let Err(error) = mirror_owned_write(host, database_id, signed) else {
        return Ok(());
    };
    tracing::warn!(
        target: "lastdbd::log_sync",
        %error,
        "owned write was not mirrored to the signed log"
    );
    if let Some(store) = crate::log_sync::owned_store(host, database_id) {
        crate::log_sync::revert_unsigned(host, database_id, &store, &keys_of(signed)).await;
    }
    Err(HostError::new(
        503,
        "signed history mirror was not confirmed",
    ))
}
