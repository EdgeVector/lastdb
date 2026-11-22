//! Birth material for databases this node minted.
//!
//! A database id is the verifying key of a key made at creation. That key
//! signs `owner ‖ db_salt` once and is then dropped. The signature and salt are
//! kept here so the signed birth entry can be authored later. The entry is a
//! pure function of this file and the device id, so authoring it again yields
//! the same bytes and never equivocates against a copy already on a relay.

use app_identity_crypto::SigningKey;
use mutation_log::{
    author_birth, birth_proof_bytes, encode_signature_b64url, AuthorCtx, Hex32, NodeKey,
    SignedEntry,
};
use serde::{Deserialize, Serialize};

use super::chain_author::AuthorIdentity;
use crate::profile::private_fs as disk;

const LIMIT: usize = 4096;

#[derive(Serialize, Deserialize)]
pub(crate) struct BirthMaterial {
    db_salt: Hex32,
    birth_signature: String,
    authored_at_ms: u64,
}

fn path(home: &std::path::Path, database_id: &str) -> std::path::PathBuf {
    home.join("log-sync").join(database_id).join("birth.json")
}

/// Record birth material for a database just minted from `db_seed`.
pub(crate) fn mint(
    home: &std::path::Path,
    database_id: &str,
    owner: [u8; 32],
    db_seed: &[u8; 32],
) -> Result<(), String> {
    let db_key = SigningKey::from_bytes(db_seed);
    if app_identity_crypto::hex_lower(&db_key.verifying_key().to_bytes()) != database_id {
        return Err("birth key does not match the database id".into());
    }
    let mut salt = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut salt);
    let db_salt = Hex32::from_bytes(salt);
    let proof = birth_proof_bytes(NodeKey::from_bytes(owner), db_salt);
    let material = BirthMaterial {
        db_salt,
        birth_signature: encode_signature_b64url(&app_identity_crypto::sign(&db_key, &proof)),
        authored_at_ms: fold_db::clock::unix_millis(),
    };
    let bytes = serde_json::to_vec(&material).map_err(|_| "cannot encode birth material")?;
    disk::write_private(&path(home, database_id), &bytes)
        .map_err(|_| "cannot write birth material".into())
}

pub(crate) fn load(
    home: &std::path::Path,
    database_id: &str,
) -> Result<Option<BirthMaterial>, String> {
    let Some(bytes) = disk::read_limited(&path(home, database_id), LIMIT)
        .map_err(|_| "cannot read birth material")?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| "birth material is invalid".into())
}

/// The signed birth entry (`writer_seq` 1) for this node's device stream.
pub(crate) fn entry(
    material: &BirthMaterial,
    ctx: &AuthorCtx,
    identity: &AuthorIdentity,
) -> Result<SignedEntry, String> {
    let ctx = AuthorCtx {
        authored_at_ms: material.authored_at_ms,
        ..ctx.clone()
    };
    author_birth(
        &ctx,
        &identity.signing_key,
        identity.node_key,
        material.db_salt,
        material.birth_signature.clone(),
    )
    .map_err(|error| error.to_string())
}
