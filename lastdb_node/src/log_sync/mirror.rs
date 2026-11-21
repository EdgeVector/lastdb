//! Mirror an owned shareable `/v1/write` onto the signed history outbox.
//!
//! FoldDB remains the publisher's local store of truth for the write that just
//! committed. The signed `data` entry lets receivers materialize the same
//! record after they pull the publisher stream. Birth is authored here when
//! this is the first chain entry for the database.

use fold_db::schema::types::MutationType;
use mutation_log::{
    author_data, data_record, entry_hash, AcceptOutcome, AuthorCtx, DatabaseId, StreamKind,
};
use serde_json::Value;

use super::birth;
use super::chain_author;
use super::device;
use super::history_store;
use super::wake;
use crate::host::Host;

/// One FoldDB mutation to carry as a signed direct `data` entry.
pub struct OwnedMutation {
    pub schema: String,
    pub record_key: String,
    pub op: MutationType,
    pub fields: Option<Value>,
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
    let identity = chain_author::identity_of(host).map_err(|e| e.reply().message.to_owned())?;
    let _ = device::device_id(&host.home)?;
    let histories = super::histories_for(&host.home);
    let database = DatabaseId::from_hex(database_id).map_err(|_| "database_id is not hex32")?;
    histories.with_mut(database_id, |history| {
        ensure_birth(history, &host.home, database_id, database, &identity)?;
        for item in mutations {
            author_one(history, &host.home, database_id, database, &identity, item)?;
        }
        Ok::<(), String>(())
    })??;
    wake::request_wake(&host.home);
    Ok(())
}

fn ensure_birth(
    history: &mut mutation_log::VerifiedHistory,
    home: &std::path::Path,
    database_id: &str,
    database: DatabaseId,
    identity: &super::apply::AuthorIdentity,
) -> Result<(), String> {
    if history.owner().is_some() {
        return Ok(());
    }
    let material = birth::load(home, database_id)?
        .ok_or_else(|| "this database has no signed birth on this node".to_owned())?;
    let stream = mutation_log::StreamId {
        node_key: identity.node_key,
        device_id: identity.device_id,
    };
    let ctx = AuthorCtx {
        database_id: database,
        node_key: identity.node_key,
        device_id: identity.device_id,
        stream_head: history.stream_head(&stream),
        seen_heads: Vec::new(),
        seen_lamports: Vec::new(),
        authored_at_ms: 0, // birth::entry replaces this from birth material
    };
    let entry = birth::entry(&material, &ctx, identity)?;
    accept_outbox(history, home, database_id, entry)
}

fn author_one(
    history: &mut mutation_log::VerifiedHistory,
    home: &std::path::Path,
    database_id: &str,
    database: DatabaseId,
    identity: &super::apply::AuthorIdentity,
    item: &OwnedMutation,
) -> Result<(), String> {
    let stream = mutation_log::StreamId {
        node_key: identity.node_key,
        device_id: identity.device_id,
    };
    let ctx = AuthorCtx {
        database_id: database,
        node_key: identity.node_key,
        device_id: identity.device_id,
        stream_head: history.stream_head(&stream),
        seen_heads: Vec::new(),
        seen_lamports: Vec::new(),
        authored_at_ms: fold_db::clock::unix_millis(),
    };
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
    let entry = author_data(&ctx, &identity.signing_key, body).map_err(|e| e.to_string())?;
    accept_outbox(history, home, database_id, entry)
}

fn accept_outbox(
    history: &mut mutation_log::VerifiedHistory,
    home: &std::path::Path,
    database_id: &str,
    entry: mutation_log::SignedEntry,
) -> Result<(), String> {
    let hash = entry_hash(&entry).map_err(|e| e.to_string())?;
    match history.accept_entry(entry.clone()) {
        AcceptOutcome::Accept { .. } => {
            history_store::append_authored(home, database_id, &entry)?;
            history.outbox_mut().enqueue(hash, entry);
            history_store::persist_outbox(home, database_id, history.outbox())
        }
        AcceptOutcome::Pending { reason } => {
            Err(format!("authored entry pending locally: {reason:?}"))
        }
        AcceptOutcome::Reject { reason } => Err(format!("authored entry rejected: {reason:?}")),
    }
}
