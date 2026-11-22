//! After FoldDB commits an `/v1/write` to an owned database or to the personal
//! database, mirror it onto the signed log.

use fold_db::schema::types::KeyValue;

use crate::exec::reply::HostError;
use crate::exec::schema_routes::resolve_schema_name;
use crate::host::Host;
use crate::log_sync::{mirror_owned_write, write_target, OwnedMutation};

/// Capture the schema and key of each written record for the signed-history
/// mirror. The signed entry carries the record as the store holds it after the
/// commit, so the request's fields and alternates are not copied here.
pub(super) fn plan<'a>(
    items: impl IntoIterator<Item = (&'a str, &'a KeyValue)>,
) -> Vec<OwnedMutation> {
    items
        .into_iter()
        .filter_map(|(schema, key)| {
            let record_key =
                mutation_log::join_record_key(key.hash.as_deref()?, key.range.as_deref())?;
            Some(OwnedMutation {
                schema: schema.to_owned(),
                record_key,
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
    let Err(error) = mirror_owned_write(host, &target.id, &signed).await else {
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

/// Refuse a file that the personal database cannot sign: its sealed blob would
/// not fit one relay blob. A file in a database that is not the personal one is
/// not signed, so the limit does not apply to it.
pub(super) fn require_file_fits(
    host: &Host,
    selector: Option<&str>,
    sizes: impl IntoIterator<Item = u64>,
) -> Result<(), HostError> {
    let personal = write_target(host, selector).is_some_and(|target| target.personal);
    if personal
        && sizes
            .into_iter()
            .any(|size| size > crate::log_sync::FILE_SIZE_MAX)
    {
        return Err(super::bad(format!(
            "a file in the personal database may be at most {} bytes",
            crate::log_sync::FILE_SIZE_MAX
        )));
    }
    Ok(())
}
