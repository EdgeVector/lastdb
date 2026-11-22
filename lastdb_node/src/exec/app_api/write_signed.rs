//! After FoldDB commits an `/v1/write` to an owned database or to the personal
//! database, mirror it onto the signed log.

use std::collections::HashMap;

use fold_db::schema::types::{KeyValue, MutationType};
use serde_json::Value;

use crate::exec::reply::HostError;
use crate::exec::schema_routes::resolve_schema_name;
use crate::host::Host;
use crate::log_sync::{mirror_owned_write, write_target, OwnedMutation};

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
fn keys_of(signed: &[OwnedMutation]) -> Vec<(String, String)> {
    signed
        .iter()
        .map(|item| (item.schema.clone(), item.record_key.clone()))
        .collect()
}

/// Author the signed entries of an owned or personal write; no-op for a
/// received database, which a write never reaches.
///
/// The store validated and committed the write first. When its entries cannot
/// all be signed, the unsigned records are reverted to the history, so the
/// store never holds a record the history does not. The personal database
/// names its schemas by their store name, which the owner socket may have
/// matched loosely, so they are resolved here.
pub(super) async fn mirror_committed(
    host: &Host,
    selector: Option<&str>,
    mut signed: Vec<OwnedMutation>,
) -> Result<(), HostError> {
    let Some(target) = write_target(host, selector) else {
        return Ok(());
    };
    if target.personal {
        for item in &mut signed {
            if let Some(name) = resolve_schema_name(host, &item.schema)? {
                item.schema = name;
            }
        }
    }
    let Err(error) = mirror_owned_write(host, &target.id, &signed) else {
        return Ok(());
    };
    tracing::warn!(
        target: "lastdbd::log_sync",
        %error,
        "owned write was not mirrored to the signed log"
    );
    if let Some(store) = crate::log_sync::owned_store(host, &target.id) {
        crate::log_sync::revert_unsigned(host, &target.id, &store, &keys_of(&signed)).await;
    }
    Err(HostError::new(
        503,
        "signed history mirror was not confirmed",
    ))
}
