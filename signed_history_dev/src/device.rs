//! Local device identity and AcceptEntry authoring helpers.

use app_identity_crypto::{hex_lower, sign, SigningKey};
use mutation_log::{
    author_birth, author_data, author_grant, birth_proof_bytes, data_record,
    encode_signature_b64url, entry_hash, grant_raise, grant_reduce, AcceptOutcome, AuthorCtx,
    DatabaseId, DeviceId, EntryHash, Grantee, Hex32, NodeKey, RejectReason, RevocationCutoff, Role,
    SignedEntry, StreamHead, StreamKind, VerifiedHistory,
};
use rand::RngCore;
use uuid::Uuid;

/// One Ed25519 writer stream.
pub struct Device {
    pub signing_key: SigningKey,
    pub node_key: NodeKey,
    pub device_id: DeviceId,
}

impl Device {
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut seed);
        let signing_key = SigningKey::from_bytes(&seed);
        let node_key = NodeKey::from_bytes(signing_key.verifying_key().to_bytes());
        let device_id = DeviceId::from_uuid(Uuid::new_v4());
        Self {
            signing_key,
            node_key,
            device_id,
        }
    }

    pub fn node_key_hex(&self) -> String {
        self.node_key.to_hex()
    }

    pub fn device_id_str(&self) -> String {
        self.device_id.to_string()
    }

    pub fn author_ctx(
        &self,
        database_id: DatabaseId,
        stream_head: Option<StreamHead>,
        seen_heads: Vec<EntryHash>,
        seen_lamports: Vec<u64>,
        authored_at_ms: u64,
    ) -> AuthorCtx {
        AuthorCtx {
            database_id,
            node_key: self.node_key,
            device_id: self.device_id,
            stream_head,
            seen_heads,
            seen_lamports,
            authored_at_ms,
        }
    }
}

/// Fresh database birth material (self-certifying id = db verifying key).
pub struct DbBirth {
    pub database_id: DatabaseId,
    pub db_salt: Hex32,
    pub birth_signature: String,
}

impl DbBirth {
    /// Mint a self-certifying database id and sign `owner ‖ db_salt` with the db key.
    pub fn generate(owner: NodeKey) -> Self {
        let mut seed = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut seed);
        let db_key = SigningKey::from_bytes(&seed);
        let database_id = DatabaseId::from_bytes(db_key.verifying_key().to_bytes());
        let mut salt = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut salt);
        let db_salt = Hex32::from_bytes(salt);
        let proof = birth_proof_bytes(owner, db_salt);
        let birth_signature = encode_signature_b64url(&sign(&db_key, &proof));
        Self {
            database_id,
            db_salt,
            birth_signature,
        }
    }
}

/// Shared fields for a direct data write.
pub struct DataWrite<'a> {
    pub database_id: DatabaseId,
    pub namespace: &'a str,
    pub record_key: &'a str,
    pub value: &'a [u8],
    pub authored_at_ms: u64,
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn nonce_hex() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex_lower(&bytes)
}

/// Accept and return Ok, or Err with a short reason.
pub fn must_accept(history: &mut VerifiedHistory, entry: SignedEntry) -> Result<EntryHash, String> {
    let hash = entry_hash(&entry).map_err(|e| e.to_string())?;
    match history.accept_entry(entry) {
        AcceptOutcome::Accept { .. } => Ok(hash),
        AcceptOutcome::Pending { reason } => Err(format!("pending {reason:?}")),
        AcceptOutcome::Reject { reason } => Err(format!("reject {reason:?}")),
    }
}

/// AcceptEntry must Reject; return the reason.
pub fn must_reject(
    history: &mut VerifiedHistory,
    entry: SignedEntry,
) -> Result<RejectReason, String> {
    match history.accept_entry(entry) {
        AcceptOutcome::Reject { reason } => Ok(reason),
        other => Err(format!("expected Reject, got {other:?}")),
    }
}

pub fn stream_head_of(history: &VerifiedHistory, device: &Device) -> Option<StreamHead> {
    history.stream_head(&VerifiedHistory::stream(device.node_key, device.device_id))
}

/// Birth + accept on a fresh history.
pub fn bootstrap_owner(
    owner: &Device,
    birth: &DbBirth,
    authored_at_ms: u64,
) -> Result<(VerifiedHistory, SignedEntry), String> {
    let mut history = VerifiedHistory::new(birth.database_id);
    let ctx = owner.author_ctx(
        birth.database_id,
        None,
        Vec::new(),
        Vec::new(),
        authored_at_ms,
    );
    let entry = author_birth(
        &ctx,
        &owner.signing_key,
        owner.node_key,
        birth.db_salt,
        birth.birth_signature.clone(),
    )
    .map_err(|e| e.to_string())?;
    must_accept(&mut history, entry.clone())?;
    Ok((history, entry))
}

/// Publisher direct data write + accept.
pub fn owner_write(
    history: &mut VerifiedHistory,
    owner: &Device,
    write: DataWrite<'_>,
) -> Result<SignedEntry, String> {
    let head = stream_head_of(history, owner);
    let ctx = owner.author_ctx(
        write.database_id,
        head,
        Vec::new(),
        Vec::new(),
        write.authored_at_ms,
    );
    let body = data_record(
        StreamKind::Direct,
        write.namespace,
        write.record_key,
        write.value,
    );
    let entry = author_data(&ctx, &owner.signing_key, body).map_err(|e| e.to_string())?;
    must_accept(history, entry.clone())?;
    Ok(entry)
}

/// Publisher grant raise (first grant, no cutoffs).
pub fn owner_grant_write(
    history: &mut VerifiedHistory,
    owner: &Device,
    database_id: DatabaseId,
    member: NodeKey,
    authored_at_ms: u64,
) -> Result<SignedEntry, String> {
    let head = stream_head_of(history, owner);
    let ctx = owner.author_ctx(database_id, head, Vec::new(), Vec::new(), authored_at_ms);
    let body = grant_raise(Grantee::Member(member), Role::Write);
    let entry = author_grant(&ctx, &owner.signing_key, body).map_err(|e| e.to_string())?;
    must_accept(history, entry.clone())?;
    Ok(entry)
}

/// Publisher revoke with a role-reduction cutoff.
pub fn owner_revoke(
    history: &mut VerifiedHistory,
    owner: &Device,
    database_id: DatabaseId,
    member: NodeKey,
    cutoff: RevocationCutoff,
    authored_at_ms: u64,
) -> Result<SignedEntry, String> {
    let head = stream_head_of(history, owner);
    let ctx = owner.author_ctx(database_id, head, Vec::new(), Vec::new(), authored_at_ms);
    let body = grant_reduce(Grantee::Member(member), Role::Revoked, vec![cutoff]);
    let entry = author_grant(&ctx, &owner.signing_key, body).map_err(|e| e.to_string())?;
    must_accept(history, entry.clone())?;
    Ok(entry)
}

/// Member direct write bound to a publisher grant in `seen_heads`.
pub fn member_write(
    history: &mut VerifiedHistory,
    member: &Device,
    grant: &SignedEntry,
    write: DataWrite<'_>,
) -> Result<SignedEntry, String> {
    let entry = author_member_write(history, member, grant, write)?;
    must_accept(history, entry.clone())?;
    Ok(entry)
}

/// Author a member direct write without accepting (for reject proofs).
pub fn author_member_write(
    history: &VerifiedHistory,
    member: &Device,
    grant: &SignedEntry,
    write: DataWrite<'_>,
) -> Result<SignedEntry, String> {
    let grant_hash = entry_hash(grant).map_err(|e| e.to_string())?;
    let grant_hdr = grant.header();
    let head = stream_head_of(history, member);
    let ctx = member.author_ctx(
        write.database_id,
        head,
        vec![grant_hash],
        vec![grant_hdr.lamport],
        write.authored_at_ms,
    );
    let mut body = data_record(
        StreamKind::Direct,
        write.namespace,
        write.record_key,
        write.value,
    );
    body.grant_at_device = Some(grant_hdr.device_id);
    body.grant_at_seq = Some(grant_hdr.writer_seq);
    author_data(&ctx, &member.signing_key, body).map_err(|e| e.to_string())
}

/// Tamper the wire signature so verification fails.
pub fn tamper_signature(entry: &mut SignedEntry) {
    let sig = &mut entry.header_mut().signature;
    let mut chars: Vec<char> = sig.chars().collect();
    if let Some(c) = chars.first_mut() {
        *c = if *c == 'A' { 'B' } else { 'A' };
        *sig = chars.into_iter().collect();
    }
}
