//! After FoldDB commits an owned `/v1/write`, mirror it onto the signed outbox.

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
            let record_key = key.hash.as_ref()?.clone();
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

/// Author signed `data` for a shareable owned write; no-op when no database id.
pub(super) fn mirror_committed(
    host: &Host,
    database_id: Option<&str>,
    signed: &[OwnedMutation],
) -> Result<(), HostError> {
    let Some(database_id) = database_id else {
        return Ok(());
    };
    mirror_owned_write(host, database_id, signed).map_err(|error| {
        tracing::warn!(
            target: "lastdbd::log_sync",
            %error,
            "owned write was not mirrored to the signed outbox"
        );
        HostError::new(503, "signed history mirror was not confirmed")
    })
}
