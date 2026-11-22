//! Mirror an owned shareable `/v1/write` onto the signed history log.
//!
//! FoldDB remains the publisher's local store of truth for the write that just
//! committed. The signed `data` entry lets receivers materialize the same
//! record after they pull the publisher stream. Birth is authored here when
//! this is the first chain entry for the database. A `schema` entry is authored
//! before data when the namespace is new or its FoldDB definition has changed.

use std::path::Path;

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::MutationType;
use mutation_log::{
    author_data, author_schema, data_record, AcceptOutcome, AuthorCtx, DatabaseId, SchemaBody,
    StreamKind, VerifiedHistory,
};
use serde_json::Value;

use super::birth;
use super::chain_author;
use super::chain_author::AuthorIdentity;
use super::device;
use super::history_store;
use super::materialize;
use super::wake;
use crate::host::Host;

/// One FoldDB mutation to carry as a signed direct `data` entry.
pub struct OwnedMutation {
    pub schema: String,
    pub record_key: String,
    pub op: MutationType,
    pub fields: Option<Value>,
}

struct MirrorCtx<'a> {
    home: &'a Path,
    database_id: &'a str,
    database: DatabaseId,
    identity: &'a AuthorIdentity,
    store: &'a FoldDB,
}

/// Author signed `data` entries for an owned shareable write and wake sync.
///
/// No-op when this database has no birth material (not a signed-log share).
pub fn mirror_owned_write(
    host: &Host,
    database_id: &str,
    mutations: &[OwnedMutation],
) -> Result<(), String> {
    if mutations.is_empty() {
        return Ok(());
    }
    if birth::load(&host.home, database_id)?.is_none() {
        return Ok(());
    }
    let store = materialize::store_for(host, database_id)
        .ok_or_else(|| "no FoldDB store is open for this database".to_owned())?;
    let identity = chain_author::identity_of(host).map_err(|e| e.reply().message.to_owned())?;
    let _ = device::device_id(&host.home)?;
    let histories = super::histories_for(&host.home);
    let database = DatabaseId::from_hex(database_id).map_err(|_| "database_id is not hex32")?;
    let ctx = MirrorCtx {
        home: &host.home,
        database_id,
        database,
        identity: &identity,
        store: &store,
    };
    histories.with_mut(database_id, |history| {
        ensure_birth(history, &ctx)?;
        for item in mutations {
            ensure_schema(history, &ctx, &item.schema)?;
            author_one(history, &ctx, item)?;
        }
        Ok::<(), String>(())
    })??;
    wake::request_wake(&host.home);
    Ok(())
}

fn ensure_birth(history: &mut VerifiedHistory, ctx: &MirrorCtx<'_>) -> Result<(), String> {
    if history.owner().is_some() {
        return Ok(());
    }
    let material = birth::load(ctx.home, ctx.database_id)?
        .ok_or_else(|| "this database has no signed birth on this node".to_owned())?;
    let author = author_ctx(history, ctx, 0);
    let entry = birth::entry(&material, &author, ctx.identity)?;
    sign(history, ctx, &entry)
}

/// Publish every schema this publisher's store declares and its history does
/// not carry yet, or carries with an older definition.
///
/// A receiver's store holds only the schemas the signed history declares. A
/// schema with no record yet would otherwise never reach a receiver, and a
/// read of it there would name an unknown schema. A write still publishes its
/// own schema first (see [`mirror_owned_write`]), so a schema entry always
/// precedes the data that needs it.
///
/// No-op without birth material, and until the birth is in the history: the
/// first write or grant authors the birth.
pub(super) fn publish_declared_schemas(host: &Host, database_id: &str) -> Result<(), String> {
    if birth::load(&host.home, database_id)?.is_none() {
        return Ok(());
    }
    let Some(store) = materialize::store_for(host, database_id) else {
        return Ok(());
    };
    let mut names: Vec<String> = store
        .schema_manager()
        .get_schemas()
        .map_err(|e| e.to_string())?
        .into_keys()
        .collect();
    names.sort();
    let identity = chain_author::identity_of(host).map_err(|e| e.reply().message.to_owned())?;
    let database = DatabaseId::from_hex(database_id).map_err(|_| "database_id is not hex32")?;
    let ctx = MirrorCtx {
        home: &host.home,
        database_id,
        database,
        identity: &identity,
        store: &store,
    };
    super::histories_for(&host.home).with_mut(database_id, |history| {
        if history.owner().is_none() {
            return Ok(());
        }
        for name in &names {
            ensure_schema(history, &ctx, name)?;
        }
        Ok::<(), String>(())
    })?
}

fn ensure_schema(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
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
    let author = author_ctx(history, ctx, fold_db::clock::unix_millis());
    let body = SchemaBody {
        namespace: name.to_owned(),
        definition,
    };
    let entry =
        author_schema(&author, &ctx.identity.signing_key, body).map_err(|e| e.to_string())?;
    sign(history, ctx, &entry)
}

fn schema_definition_matches(prior: &[u8], current: &fold_db::schema::types::Schema) -> bool {
    match serde_json::from_slice::<fold_db::schema::types::Schema>(prior) {
        Ok(held) => held == *current,
        Err(_) => false,
    }
}

fn author_one(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    item: &OwnedMutation,
) -> Result<(), String> {
    let author = author_ctx(history, ctx, fold_db::clock::unix_millis());
    let value = if item.op == MutationType::Delete {
        Vec::new()
    } else {
        let fields = item
            .fields
            .as_ref()
            .ok_or_else(|| "owned write is missing record fields".to_owned())?;
        serde_json::to_vec(fields).map_err(|_| "cannot encode record fields".to_owned())?
    };
    let body = data_record(
        StreamKind::Direct,
        item.schema.clone(),
        &item.record_key,
        &value,
    );
    let entry = author_data(&author, &ctx.identity.signing_key, body).map_err(|e| e.to_string())?;
    sign(history, ctx, &entry)
}

fn author_ctx(history: &VerifiedHistory, ctx: &MirrorCtx<'_>, authored_at_ms: u64) -> AuthorCtx {
    let stream = mutation_log::StreamId {
        node_key: ctx.identity.node_key,
        device_id: ctx.identity.device_id,
    };
    AuthorCtx {
        database_id: ctx.database,
        node_key: ctx.identity.node_key,
        device_id: ctx.identity.device_id,
        stream_head: history.stream_head(&stream),
        seen_heads: Vec::new(),
        seen_lamports: Vec::new(),
        authored_at_ms,
    }
}

/// Accept `entry` into the history and append it to the log. A rejected or
/// pending entry is an error, and the log is unchanged.
fn sign(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    entry: &mutation_log::SignedEntry,
) -> Result<(), String> {
    match history_store::commit_authored(ctx.home, history, entry)? {
        AcceptOutcome::Accept { .. } => Ok(()),
        AcceptOutcome::Pending { reason } => {
            Err(format!("authored entry pending locally: {reason:?}"))
        }
        AcceptOutcome::Reject { reason } => Err(format!("authored entry rejected: {reason:?}")),
    }
}
