//! Sign the writes of the personal database as sealed entries.
//!
//! One write call is one entry: a `data` entry for one record, a `bundle` entry
//! for several. A schema the write needs and the history does not carry yet is
//! signed first, as its own `schema` entry. Every payload is sealed before it is
//! signed (see `sealed`), so the relay sees the entry kinds, the sizes and the
//! times, and nothing else.
//!
//! A bundle holds its changes in a sealed blob. The blob is kept in
//! `log-sync/<database>/blobs` before the entry is signed, and the upload sends
//! it to the relay before the entry.

use std::collections::{BTreeMap, BTreeSet};

use mutation_log::{
    author_bundle, author_data, author_schema, encode_bundle_changes, encode_named,
    encode_record_payload, AuthorCtx, BundleBody, BundleChange, DataBody, SchemaBody, StreamKind,
    VerifiedHistory, BUNDLE_BLOB_SIZE_MAX, BUNDLE_CHANGE_COUNT_MAX, SEALED_NAMESPACE,
};

use super::blobs;
use super::mirror::{
    author_ctx, encode_value, schema_definition_matches, sign, sign_bundle, MirrorCtx,
    OwnedMutation,
};
use super::sealed::Sealer;

/// Sign the schemas and the changes of one write call.
///
/// Two mutations of one record in one call collapse to the last one, which is
/// the state the store holds after the commit.
pub(super) fn author(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    sealer: &Sealer,
    items: &[OwnedMutation],
) -> Result<(), String> {
    let mut latest: BTreeMap<(String, String), Vec<u8>> = BTreeMap::new();
    for item in items {
        latest.insert(
            (item.schema.clone(), item.record_key.clone()),
            encode_value(item)?,
        );
    }
    let schemas: BTreeSet<&str> = latest.keys().map(|(schema, _)| schema.as_str()).collect();
    for name in schemas {
        ensure_schema(history, ctx, sealer, name)?;
    }
    let changes: Vec<BundleChange> = latest
        .into_iter()
        .map(|((namespace, record_key), value)| BundleChange {
            namespace,
            record_key,
            value,
        })
        .collect();
    if let [one] = changes.as_slice() {
        author_record(history, ctx, sealer, one)
    } else {
        author_changes(history, ctx, sealer, &changes)
    }
}

/// Seal `plain` for the next entry of this device's stream.
fn seal_next(
    ctx: &MirrorCtx<'_>,
    sealer: &Sealer,
    author: &AuthorCtx,
    plain: &[u8],
) -> Result<Vec<u8>, String> {
    let writer_seq = author.stream_head.map_or(1, |head| head.writer_seq + 1);
    sealer.seal(
        ctx.identity.node_key,
        ctx.identity.device_id,
        writer_seq,
        plain,
    )
}

/// Sign the schema as a `schema` entry unless the history already carries this
/// definition under this name.
fn ensure_schema(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    sealer: &Sealer,
    name: &str,
) -> Result<(), String> {
    let schema = ctx
        .store
        .schema_manager()
        .get_schema_metadata(name)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("schema {name} is not declared on this store"))?;
    if let Some(prior) = history.schemas().get(name) {
        if schema_definition_matches(prior, &schema) {
            return Ok(());
        }
    }
    let definition =
        serde_json::to_vec(&schema).map_err(|_| "cannot encode schema definition".to_owned())?;
    let plain = encode_named(name, &definition).ok_or("schema name is not a valid namespace")?;
    let author = author_ctx(history, ctx, fold_db::clock::unix_millis());
    let body = SchemaBody {
        namespace: SEALED_NAMESPACE.to_owned(),
        definition: seal_next(ctx, sealer, &author, &plain)?,
    };
    let entry =
        author_schema(&author, &ctx.identity.signing_key, body).map_err(|e| e.to_string())?;
    sign(history, ctx, &entry)
}

/// Sign one record as a `data` entry.
fn author_record(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    sealer: &Sealer,
    change: &BundleChange,
) -> Result<(), String> {
    let record = encode_record_payload(&change.record_key, &change.value);
    let plain = encode_named(&change.namespace, &record).ok_or("schema name is not valid")?;
    let author = author_ctx(history, ctx, fold_db::clock::unix_millis());
    let body = DataBody {
        stream: StreamKind::Direct,
        namespace: SEALED_NAMESPACE.to_owned(),
        payload: seal_next(ctx, sealer, &author, &plain)?,
        file: None,
        incorporated_from: None,
        grant_at_device: None,
        grant_at_seq: None,
    };
    let entry = author_data(&author, &ctx.identity.signing_key, body).map_err(|e| e.to_string())?;
    sign(history, ctx, &entry)
}

/// Sign several records as one `bundle` entry. The sealed blob is kept first.
fn author_changes(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    sealer: &Sealer,
    changes: &[BundleChange],
) -> Result<(), String> {
    let change_count = u32::try_from(changes.len())
        .ok()
        .filter(|count| *count <= BUNDLE_CHANGE_COUNT_MAX)
        .ok_or("a write holds more changes than one bundle may")?;
    let plain = encode_bundle_changes(changes).ok_or("a change is too large for a bundle")?;
    let author = author_ctx(history, ctx, fold_db::clock::unix_millis());
    let blob = seal_next(ctx, sealer, &author, &plain)?;
    let blob_size = u64::try_from(blob.len())
        .ok()
        .filter(|size| *size <= BUNDLE_BLOB_SIZE_MAX)
        .ok_or("a write is too large for one bundle")?;
    let blob_sha256 = blobs::digest(&blob);
    blobs::save(ctx.home, ctx.database_id, &blob_sha256, &blob)?;
    let body = BundleBody {
        blob_sha256,
        blob_size,
        change_count,
        grant_at_device: None,
        grant_at_seq: None,
    };
    let entry =
        author_bundle(&author, &ctx.identity.signing_key, body).map_err(|e| e.to_string())?;
    sign_bundle(history, ctx, &entry, changes)
}
