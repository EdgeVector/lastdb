//! Bootstrap an existing local database into signed history.
//!
//! No migration / dual path: emit birth, then schema declarations, then
//! ordinary signed `data` mutations for each current record, then fresh grants.

use app_identity_crypto::SigningKey;

use crate::accept::AcceptOutcome;
use crate::author::{
    author_birth, author_data, author_grant, author_schema, data_record, grant_raise, AuthorCtx,
};
use crate::entry::{SchemaBody, SignedEntry};
use crate::history::VerifiedHistory;
use crate::ids::{DatabaseId, DeviceId, Hex32, NodeKey};
use crate::kinds::{Grantee, Role, StreamKind};
use crate::wire::EncodeError;

/// One schema declaration to emit before record data.
#[derive(Clone, Debug)]
pub struct BootstrapSchema {
    /// Namespace (FoldDB schema name).
    pub namespace: String,
    /// UTF-8 JSON schema definition.
    pub definition: Vec<u8>,
}

/// One materialized record to emit as a signed data entry.
#[derive(Clone, Debug)]
pub struct BootstrapRecord {
    /// Namespace.
    pub namespace: String,
    /// Record key.
    pub record_key: String,
    /// Value bytes (empty = tombstone).
    pub value: Vec<u8>,
}

/// Fresh member grant to emit after records.
#[derive(Clone, Debug)]
pub struct BootstrapGrant {
    /// Grantee.
    pub grantee: Grantee,
    /// Role (`read` / `submit` / `write`; not a reduction).
    pub role: Role,
}

/// Inputs for [`bootstrap_from_records`].
pub struct BootstrapParams<'a> {
    /// Database id.
    pub database_id: DatabaseId,
    /// Owner / publisher signing key.
    pub signing_key: &'a SigningKey,
    /// Owner public key (must match signing key).
    pub owner: NodeKey,
    /// Publisher device id for the new stream.
    pub device_id: DeviceId,
    /// Database salt.
    pub db_salt: Hex32,
    /// Birth signature: db-key sig over `owner ‖ db_salt` (base64url 86).
    pub birth_signature: String,
    /// Wall clock for birth.
    pub authored_at_ms: u64,
    /// Schema declarations to emit after birth (every record namespace must appear).
    pub schemas: &'a [BootstrapSchema],
    /// Current records to emit.
    pub records: &'a [BootstrapRecord],
    /// Fresh grants to emit after records.
    pub grants: &'a [BootstrapGrant],
}

/// Result of a bootstrap: verified history with birth + schemas + data + grants
/// accepted, and the same entries on the outbox.
pub struct BootstrapResult {
    /// History after accepting every authored entry.
    pub history: VerifiedHistory,
    /// Entries in author order (birth, schema…, data…, grants…).
    pub entries: Vec<SignedEntry>,
}

/// Author birth + schemas + data + grants, accept each into a fresh history, enqueue outbox.
pub fn bootstrap_from_records(
    params: &BootstrapParams<'_>,
) -> Result<BootstrapResult, BootstrapError> {
    let mut history = VerifiedHistory::new(params.database_id);
    let mut entries = Vec::new();
    let mut ctx = AuthorCtx {
        database_id: params.database_id,
        node_key: params.owner,
        device_id: params.device_id,
        stream_head: None,
        seen_heads: Vec::new(),
        seen_lamports: Vec::new(),
        authored_at_ms: params.authored_at_ms,
    };

    let birth = author_birth(
        &ctx,
        params.signing_key,
        params.owner,
        params.db_salt,
        params.birth_signature.clone(),
    )?;
    accept_and_track(&mut history, &mut entries, &mut ctx, birth)?;

    for (i, schema) in params.schemas.iter().enumerate() {
        ctx.authored_at_ms = params.authored_at_ms.saturating_add(1 + i as u64);
        let body = SchemaBody {
            namespace: schema.namespace.clone(),
            definition: schema.definition.clone(),
        };
        let entry = author_schema(&ctx, params.signing_key, body)?;
        accept_and_track(&mut history, &mut entries, &mut ctx, entry)?;
    }

    let data_base = params
        .authored_at_ms
        .saturating_add(1 + params.schemas.len() as u64);
    for (i, rec) in params.records.iter().enumerate() {
        if !history.schemas().contains_key(&rec.namespace) {
            return Err(BootstrapError::Reject(format!(
                "bootstrap record namespace {} has no schema declaration",
                rec.namespace
            )));
        }
        ctx.authored_at_ms = data_base.saturating_add(i as u64);
        let body = data_record(
            StreamKind::Direct,
            rec.namespace.clone(),
            &rec.record_key,
            &rec.value,
        );
        let entry = author_data(&ctx, params.signing_key, body)?;
        accept_and_track(&mut history, &mut entries, &mut ctx, entry)?;
    }

    let grant_base = data_base.saturating_add(params.records.len() as u64);
    for (i, g) in params.grants.iter().enumerate() {
        ctx.authored_at_ms = grant_base.saturating_add(i as u64);
        let body = grant_raise(g.grantee, g.role);
        let entry = author_grant(&ctx, params.signing_key, body)?;
        accept_and_track(&mut history, &mut entries, &mut ctx, entry)?;
    }

    Ok(BootstrapResult { history, entries })
}

fn accept_and_track(
    history: &mut VerifiedHistory,
    entries: &mut Vec<SignedEntry>,
    ctx: &mut AuthorCtx,
    entry: SignedEntry,
) -> Result<(), BootstrapError> {
    let outcome = history.accept_entry(entry.clone());
    match outcome {
        AcceptOutcome::Accept { .. } => {
            history.enqueue_outbox(entry.clone())?;
            ctx.stream_head =
                history.stream_head(&VerifiedHistory::stream(ctx.node_key, ctx.device_id));
            // Same-device bootstrap advances only via prev_hash / stream head.
            ctx.seen_heads.clear();
            ctx.seen_lamports.clear();
            entries.push(entry);
            Ok(())
        }
        AcceptOutcome::Pending { reason } => Err(BootstrapError::Pending(format!("{reason:?}"))),
        AcceptOutcome::Reject { reason } => Err(BootstrapError::Reject(format!("{reason:?}"))),
    }
}

/// Bootstrap failure.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum BootstrapError {
    /// Encode / sign failure.
    #[error(transparent)]
    Encode(#[from] EncodeError),
    /// AcceptEntry returned pending (should not happen on a fresh sequential bootstrap).
    #[error("bootstrap pending: {0}")]
    Pending(String),
    /// AcceptEntry rejected an authored entry.
    #[error("bootstrap reject: {0}")]
    Reject(String),
}
