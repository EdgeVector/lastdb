//! Mirror an owned `/v1/write` onto the signed history log.
//!
//! FoldDB remains the owner's local store of truth for the write that just
//! committed. The signed entries let other nodes materialize the same records
//! after they pull the stream. Birth is authored here when this is the first
//! chain entry for the database. A `schema` entry is authored before data when
//! the namespace is new or its FoldDB definition has changed.
//!
//! A shareable database signs one `data` entry for each mutation, in clear.
//! The personal database signs sealed entries, one for each write call: see
//! `mirror_sealed`.

use std::path::Path;

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::MutationType;
use mutation_log::{
    author_data, author_schema, data_record, AcceptOutcome, AuthorCtx, BundleChange, DatabaseId,
    DeviceId, EntryHash, SchemaBody, StreamKind, VerifiedHistory,
};
use serde_json::Value;

use super::birth::{self, BirthMaterial};
use super::chain_author;
use super::chain_author::AuthorIdentity;
use super::device;
use super::history_store;
use super::materialize;
use super::mirror_sealed;
use super::sealed::Sealer;
use super::wake;
use crate::host::Host;

/// One FoldDB mutation to carry as a signed direct `data` entry.
pub struct OwnedMutation {
    pub schema: String,
    pub record_key: String,
    pub op: MutationType,
    pub fields: Option<Value>,
}

/// What only the personal database has: the sealer of its payloads, and the
/// device id of the stream that holds its birth.
pub(super) struct Personal<'a> {
    pub(super) sealer: &'a Sealer,
    pub(super) birth_device: DeviceId,
}

pub(super) struct MirrorCtx<'a> {
    pub(super) home: &'a Path,
    pub(super) database_id: &'a str,
    pub(super) database: DatabaseId,
    pub(super) identity: &'a AuthorIdentity,
    pub(super) store: &'a FoldDB,
    material: &'a BirthMaterial,
    pub(super) personal: Option<&'a Personal<'a>>,
}

/// Author signed entries for an owned write and wake sync.
///
/// No-op when this database has no birth material (not a signed-log share).
/// The personal database always has it: it comes from the node seed.
pub fn mirror_owned_write(
    host: &Host,
    database_id: &str,
    mutations: &[OwnedMutation],
) -> Result<(), String> {
    if mutations.is_empty() {
        return Ok(());
    }
    let histories = super::histories_for(host);
    let sealer = histories.sealer_of(database_id);
    let seed = zeroize::Zeroizing::new(host.keypair.secret_key_bytes());
    let material = if sealer.is_some() {
        birth::derived(&seed)
    } else {
        let Some(material) = birth::load(&host.home, database_id)? else {
            return Ok(());
        };
        material
    };
    let store = materialize::store_for(host, database_id)
        .ok_or_else(|| "no FoldDB store is open for this database".to_owned())?;
    let identity = chain_author::identity_of(host).map_err(|e| e.reply().message.to_owned())?;
    let _ = device::device_id(&host.home)?;
    let database = DatabaseId::from_hex(database_id).map_err(|_| "database_id is not hex32")?;
    let personal = sealer.as_deref().map(|sealer| Personal {
        sealer,
        birth_device: DeviceId::from_bytes(crate::personal_identity::birth_device(&seed)),
    });
    let ctx = MirrorCtx {
        home: &host.home,
        database_id,
        database,
        identity: &identity,
        store: &store,
        material: &material,
        personal: personal.as_ref(),
    };
    histories.with_mut(database_id, |history| -> Result<(), String> {
        ensure_birth(history, &ctx)?;
        if let Some(personal) = ctx.personal {
            return mirror_sealed::author(history, &ctx, personal.sealer, mutations);
        }
        for item in mutations {
            ensure_schema(history, &ctx, &item.schema)?;
            author_one(history, &ctx, item)?;
        }
        Ok(())
    })??;
    wake::request_wake(&host.home);
    Ok(())
}

/// Sign the birth when the history has none. The birth of the personal database
/// is the same bytes on every device, and it sits on its own stream, so two
/// devices that both write first cannot disagree about who signed it.
fn ensure_birth(history: &mut VerifiedHistory, ctx: &MirrorCtx<'_>) -> Result<(), String> {
    if history.owner().is_some() {
        return Ok(());
    }
    let own = author_ctx(history, ctx, 0);
    let author = match ctx.personal {
        Some(personal) => AuthorCtx {
            device_id: personal.birth_device,
            stream_head: history.stream_head(&VerifiedHistory::stream(
                ctx.identity.node_key,
                personal.birth_device,
            )),
            seen_heads: Vec::new(),
            seen_lamports: Vec::new(),
            ..own
        },
        None => own,
    };
    let entry = birth::entry(ctx.material, &author, ctx.identity)?;
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
        home: &host.home,
        database_id,
        database,
        identity: &identity,
        store: &store,
        material: &material,
        personal: None,
    };
    super::histories_for(host).with_mut(database_id, |history| {
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

pub(super) fn schema_definition_matches(
    prior: &[u8],
    current: &fold_db::schema::types::Schema,
) -> bool {
    match serde_json::from_slice::<fold_db::schema::types::Schema>(prior) {
        Ok(held) => held == *current,
        Err(_) => false,
    }
}

/// The value of a record in an entry: its fields as JSON, or nothing for a delete.
pub(super) fn encode_value(item: &OwnedMutation) -> Result<Vec<u8>, String> {
    if item.op == MutationType::Delete {
        return Ok(Vec::new());
    }
    let fields = item
        .fields
        .as_ref()
        .ok_or_else(|| "owned write is missing record fields".to_owned())?;
    serde_json::to_vec(fields).map_err(|_| "cannot encode record fields".to_owned())
}

fn author_one(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    item: &OwnedMutation,
) -> Result<(), String> {
    let author = author_ctx(history, ctx, fold_db::clock::unix_millis());
    let value = encode_value(item)?;
    let body = data_record(
        StreamKind::Direct,
        item.schema.clone(),
        &item.record_key,
        &value,
    );
    let entry = author_data(&author, &ctx.identity.signing_key, body).map_err(|e| e.to_string())?;
    sign(history, ctx, &entry)
}

/// The context of the next entry on this device's stream. An entry of the
/// personal database cites the tip of every other stream of the owner, so its
/// Lamport number is above everything this device has seen and a write made
/// after a sync wins over the writes it saw.
pub(super) fn author_ctx(
    history: &VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    authored_at_ms: u64,
) -> AuthorCtx {
    let stream = mutation_log::StreamId {
        node_key: ctx.identity.node_key,
        device_id: ctx.identity.device_id,
    };
    let (seen_heads, seen_lamports) = if ctx.personal.is_some() {
        other_stream_tips(history, ctx.identity)
    } else {
        (Vec::new(), Vec::new())
    };
    AuthorCtx {
        database_id: ctx.database,
        node_key: ctx.identity.node_key,
        device_id: ctx.identity.device_id,
        stream_head: history.stream_head(&stream),
        seen_heads,
        seen_lamports,
        authored_at_ms,
    }
}

/// The tip hash and Lamport number of every stream of the owner but this one.
fn other_stream_tips(
    history: &VerifiedHistory,
    identity: &AuthorIdentity,
) -> (Vec<EntryHash>, Vec<u64>) {
    let mut tips: Vec<(EntryHash, u64)> = history
        .stream_heads_of(identity.node_key)
        .into_iter()
        .filter(|(device_id, _)| *device_id != identity.device_id)
        .map(|(_, head)| (head.entry_hash, head.lamport))
        .collect();
    tips.sort_unstable();
    tips.into_iter().unzip()
}

/// Accept `entry` into the history and append it to the log. A rejected or
/// pending entry is an error, and the log is unchanged.
pub(super) fn sign(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    entry: &mutation_log::SignedEntry,
) -> Result<(), String> {
    accepted(history_store::commit_authored(ctx.home, history, entry)?)
}

/// [`sign`] for a `bundle` entry, whose blob the caller has kept.
pub(super) fn sign_bundle(
    history: &mut VerifiedHistory,
    ctx: &MirrorCtx<'_>,
    entry: &mutation_log::SignedEntry,
    changes: &[BundleChange],
) -> Result<(), String> {
    accepted(history_store::commit_authored_bundle(
        ctx.home, history, entry, changes,
    )?)
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
