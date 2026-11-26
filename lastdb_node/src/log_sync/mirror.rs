//! Mirror an owned `/v1/write` onto the signed history log.
//!
//! FoldDB remains the owner's local store of truth for the write that just
//! committed. The signed entries let other nodes materialize the same records
//! after they pull the stream. Birth is authored here when this is the first
//! chain entry for the database. A `schema` entry is authored before data when
//! the namespace is new or its FoldDB definition has changed.
//!
//! A shareable database signs one `data` entry for each mutation, in clear. The
//! personal database is not mirrored here: its writes reach the signed log from
//! the spool of accepted writes (see `personal`).

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::MutationType;
use mutation_log::{
    author_data, author_schema, data_record, stream_id_of, AcceptOutcome, AuthorCtx, DatabaseId,
    SchemaBody, SignedEntry, StreamKind, VerifiedHistory,
};
use serde_json::Value;

use super::birth::{self, BirthMaterial};
use super::chain_author;
use super::chain_author::AuthorIdentity;
use super::history_store;
use super::materialize;
use super::wake;
use crate::host::owned;
use crate::host::Host;
use crate::personal_seal::Sealer;
use crate::shared_seal;
use zeroize::Zeroizing;

/// One FoldDB mutation to carry as a signed direct `data` entry.
pub struct OwnedMutation {
    pub schema: String,
    pub record_key: String,
    pub op: MutationType,
    pub fields: Option<Value>,
    pub alternates: signed_history_materialize::Alternates,
}

pub(super) struct MirrorCtx<'a> {
    pub(super) database: DatabaseId,
    pub(super) identity: &'a AuthorIdentity,
    pub(super) store: &'a FoldDB,
    material: &'a BirthMaterial,
    sealer: Sealer,
}

/// Author signed entries for an owned write and wake sync.
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
    let histories = super::histories_for(host);
    let Some(material) = birth::load(&host.home, database_id)? else {
        return Ok(());
    };
    let store = materialize::store_for(host, database_id)
        .ok_or_else(|| "no FoldDB store is open for this database".to_owned())?;
    let identity = chain_author::identity_of(host).map_err(|e| e.reply().message.to_owned())?;
    let database = DatabaseId::from_hex(database_id).map_err(|_| "database_id is not hex32")?;
    let ctx = MirrorCtx {
        database,
        identity: &identity,
        store: &store,
        material: &material,
        sealer: shared_sealer(host, database_id)?,
    };
    histories.with_mut(database_id, |history| {
        history_store::commit_authored_batch(&host.home, history, |next| {
            let mut batch = Vec::with_capacity(mutations.len() + 2);
            ensure_birth(next, &ctx, &mut batch)?;
            for item in mutations {
                ensure_schema(next, &ctx, &item.schema, &mut batch)?;
                author_one(next, &ctx, item, &mut batch)?;
            }
            Ok(batch)
        })
    })??;
    wake::request_wake(&host.home);
    Ok(())
}

/// Sign the birth when the history has none.
fn ensure_birth(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    batch: &mut Vec<SignedEntry>,
) -> Result<(), String> {
    if history.owner().is_some() {
        return Ok(());
    }
    let author = author_ctx(history, ctx, 0);
    let entry = birth::entry(ctx.material, &author, ctx.identity)?;
    stage(history, batch, entry)
}

/// Publish every schema this publisher's store declares and its history does
/// not carry yet, or carries with an older definition.
pub(super) fn publish_declared_schemas(host: &Host, database_id: &str) -> Result<(), String> {
    let Some(material) = birth::load(&host.home, database_id)? else {
        return Ok(());
    };
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
        database,
        identity: &identity,
        store: &store,
        material: &material,
        sealer: shared_sealer(host, database_id)?,
    };
    super::histories_for(host).with_mut(database_id, |history| {
        if history.owner().is_none() {
            return Ok(());
        }
        history_store::commit_authored_batch(&host.home, history, |next| {
            let mut batch = Vec::new();
            for name in &names {
                ensure_schema(next, &ctx, name, &mut batch)?;
            }
            Ok(batch)
        })
    })?
}

fn ensure_schema(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    name: &str,
    batch: &mut Vec<SignedEntry>,
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
    let mut author = author_ctx(history, ctx, fold_db::clock::unix_millis());
    if let Some((hash, lamport)) = history.birth_entry() {
        author.cite(hash, lamport);
    }
    if let Some((hash, lamport)) = history.schema_entry(name) {
        author.cite(hash, lamport);
    }
    let body = SchemaBody {
        namespace: name.to_owned(),
        definition,
    };
    let body = shared_seal::seal_schema(
        &ctx.sealer,
        author.node_key,
        author.device_id,
        author.next_writer_seq(),
        body,
    )?;
    let entry =
        author_schema(&author, &ctx.identity.signing_key, body).map_err(|e| e.to_string())?;
    let opened = shared_seal::open_entry(&ctx.sealer, &entry)
        .ok_or_else(|| "sealed schema entry does not open".to_owned())?;
    stage_opened(history, batch, entry, &opened)
}

fn schema_definition_matches(prior: &[u8], current: &fold_db::schema::types::Schema) -> bool {
    match serde_json::from_slice::<fold_db::schema::types::Schema>(prior) {
        Ok(held) => held == *current,
        Err(_) => false,
    }
}

/// The value of a record in an entry: its fields (and alternates, when present)
/// as JSON, or nothing for a delete.
pub(super) fn encode_value(item: &OwnedMutation) -> Result<Vec<u8>, String> {
    if item.op == MutationType::Delete {
        return Ok(Vec::new());
    }
    let fields = item
        .fields
        .as_ref()
        .ok_or_else(|| "owned write is missing record fields".to_owned())?;
    signed_history_materialize::encode_record_value(fields, &item.alternates)
}

fn author_one(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    item: &OwnedMutation,
    batch: &mut Vec<SignedEntry>,
) -> Result<(), String> {
    let mut author = author_ctx(history, ctx, fold_db::clock::unix_millis());
    if let Some((hash, lamport)) = history.birth_entry() {
        author.cite(hash, lamport);
    }
    let key = (item.schema.clone(), item.record_key.clone());
    if let Some(winner) = history.records().get(&key) {
        author.cite(winner.entry, winner.position.lamport);
    }
    let value = encode_value(item)?;
    let body = data_record(
        StreamKind::Direct,
        item.schema.clone(),
        &item.record_key,
        &value,
    );
    let body = shared_seal::seal_data(
        &ctx.sealer,
        author.node_key,
        author.device_id,
        author.next_writer_seq(),
        body,
    )?;
    let entry = author_data(&author, &ctx.identity.signing_key, body).map_err(|e| e.to_string())?;
    let opened = shared_seal::open_entry(&ctx.sealer, &entry)
        .ok_or_else(|| "sealed data entry does not open".to_owned())?;
    stage_opened(history, batch, entry, &opened)
}

fn shared_sealer(host: &Host, database_id: &str) -> Result<Sealer, String> {
    let seed = Zeroizing::new(host.keypair.secret_key_bytes());
    let key = owned::content_encryption_key(&seed, database_id)?;
    shared_seal::for_key(database_id, key)
}

/// The context of the next entry on this device's stream.
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

fn stage(
    history: &mut VerifiedHistory,
    batch: &mut Vec<SignedEntry>,
    entry: SignedEntry,
) -> Result<(), String> {
    if history.pending_on(&stream_id_of(&entry)) {
        return Err("this stream has entries that wait for a dependency; sync first".into());
    }
    accepted(history.accept_entry(entry.clone()))?;
    batch.push(entry);
    Ok(())
}

fn stage_opened(
    history: &mut VerifiedHistory,
    batch: &mut Vec<SignedEntry>,
    entry: SignedEntry,
    opened: &SignedEntry,
) -> Result<(), String> {
    if history.pending_on(&stream_id_of(&entry)) {
        return Err("this stream has entries that wait for a dependency; sync first".into());
    }
    accepted(history.accept_entry_opened(entry.clone(), opened))?;
    batch.push(entry);
    Ok(())
}

fn accepted(outcome: AcceptOutcome) -> Result<(), String> {
    match outcome {
        AcceptOutcome::Accept { .. } => Ok(()),
        AcceptOutcome::Pending { reason } => {
            Err(format!("authored entry pending locally: {reason:?}"))
        }
        AcceptOutcome::Reject { reason } => Err(format!("authored entry rejected: {reason:?}")),
    }
}
