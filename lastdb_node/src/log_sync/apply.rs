//! Local write path: author signed entry → AcceptEntry → outbox.

use app_identity_crypto::SigningKey;
use fold_db::fold_db_core::FoldDB;
use mutation_log::{
    author_data, data_record, entry_hash, AcceptOutcome, AuthorCtx, DatabaseId, DeviceId, NodeKey,
    StreamKind,
};

use super::history_store::{self, HistoryMap};
use super::materialize;

pub(super) struct AuthorIdentity {
    pub(super) signing_key: SigningKey,
    pub(super) node_key: NodeKey,
    pub(super) device_id: DeviceId,
}

pub(super) struct DataWrite<'a> {
    pub(super) database_id: &'a str,
    pub(super) namespace: String,
    pub(super) record_key: &'a str,
    pub(super) value: &'a [u8],
    pub(super) authored_at_ms: u64,
}

/// Author a direct data mutation, accept locally, enqueue outbox.
pub(super) async fn author_and_accept_data(
    histories: &HistoryMap,
    home: &std::path::Path,
    store: &FoldDB,
    identity: &AuthorIdentity,
    write: DataWrite<'_>,
) -> Result<(), String> {
    let db = DatabaseId::from_hex(write.database_id).map_err(|_| "database_id is not hex32")?;
    histories.with_mut(write.database_id, |history| {
        let stream = mutation_log::StreamId {
            node_key: identity.node_key,
            device_id: identity.device_id,
        };
        let stream_head = history.stream_head(&stream);
        let ctx = AuthorCtx {
            database_id: db,
            node_key: identity.node_key,
            device_id: identity.device_id,
            stream_head,
            // DESIGN: Phase 1 cites no cross-stream seen_heads on local author;
            // concurrent multi-writer deps need a richer tip-selection policy.
            seen_heads: Vec::new(),
            seen_lamports: Vec::new(),
            authored_at_ms: write.authored_at_ms,
        };
        let body = data_record(
            StreamKind::Direct,
            write.namespace,
            write.record_key,
            write.value,
        );
        let entry = author_data(&ctx, &identity.signing_key, body).map_err(|e| e.to_string())?;
        let hash = entry_hash(&entry).map_err(|e| e.to_string())?;
        match history.accept_entry(entry.clone()) {
            AcceptOutcome::Accept { .. } => {
                history.outbox_mut().enqueue(hash, entry);
                history_store::persist_outbox(home, write.database_id, history.outbox())
            }
            AcceptOutcome::Pending { reason } => {
                Err(format!("authored entry pending locally: {reason:?}"))
            }
            AcceptOutcome::Reject { reason } => Err(format!("authored entry rejected: {reason:?}")),
        }
    })??;
    let history = histories.snapshot(write.database_id)?;
    materialize::apply_history_records(store, &history).await
}
