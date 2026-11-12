//! `LastDBNode` authorization for the mutation-log family.
//!
//! G verifies this digest on the same routes in dev and prod. The token carries
//! the saved connection realm. It does not add a path, a flag, or a scheme.
//! D's `NodeReadProof::digest(body)` is a different proof. That proof is not
//! sent before a verified cutover receipt, and G's current handler rejects it.

use base64::Engine as _;
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{bytes_sha256, input_digest, CloudScope, DecimalU64, Digest, Id};
use fold_db::security::Ed25519KeyPair;
use rand::RngCore;
use serde::Serialize;

const SCHEME: &str = "LastDBNode";
const DOMAIN: &str = "lastdb:share:node-read-authority:v1";

/// Inputs of one legacy node signature.
#[derive(Clone, Copy)]
pub(crate) struct Authority<'a> {
    pub keypair: &'a Ed25519KeyPair,
    pub scope: &'a CloudScope,
    pub database_id: &'a Id,
    pub path: &'a str,
    pub body: &'a [u8],
    pub member_revision: u64,
    pub now_ms: u64,
    pub environment: &'a str,
}

/// Authorization header value for one POST. A new nonce is used every call.
pub(crate) fn authorization(authority: Authority<'_>) -> Result<String, String> {
    let token = token(
        authority.keypair,
        authority.scope,
        authority.database_id,
        authority.path,
        authority.member_revision,
        authority.now_ms,
        authority.environment,
    )?;
    let digest = digest_of(&token, authority.body)?;
    let signature = authority.keypair.sign(digest.as_str().as_bytes());
    let token_json = serde_json::to_vec(&token).map_err(|_| "node read token was refused")?;
    let encoded_token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_json);
    let encoded_signature =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature.to_bytes());
    Ok(format!("{SCHEME} {encoded_token}.{encoded_signature}"))
}

fn digest_of(token: &Token, body: &[u8]) -> Result<Digest, String> {
    let body_sha256 = bytes_sha256(body);
    input_digest(
        DOMAIN,
        &(
            token.method,
            token.path.as_str(),
            token.stream.stream_id.as_str(),
            token.issued_at_ms,
            &body_sha256,
            &token.version,
            &token.stream,
            token.nonce.as_str(),
            token.member_list_revision,
            &token.node_public_key,
        ),
    )
    .map_err(|_| "node read digest was refused".into())
}

fn token(
    keypair: &Ed25519KeyPair,
    scope: &CloudScope,
    database_id: &Id,
    path: &str,
    member_revision: u64,
    now_ms: u64,
    environment: &str,
) -> Result<Token, String> {
    if now_ms == 0 || (environment != "dev" && environment != "prod") {
        return Err("node read is not bound to a realm".into());
    }
    let mut nonce_bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    Ok(Token {
        version: Version::V2,
        environment: environment.to_owned(),
        stream: Stream {
            scope: scope.clone(),
            stream_id: database_id.clone(),
        },
        method: "POST",
        path: path.to_owned(),
        issued_at_ms: DecimalU64::new(now_ms),
        nonce: hex::encode(nonce_bytes),
        member_list_revision: DecimalU64::new(member_revision),
        node_public_key: Digest::try_from(hex::encode(keypair.public_key_bytes()))
            .map_err(|_| "node key was refused")?,
    })
}

/// Fields G's verifier accepts. `database_id` stays inside `stream.stream_id`.
#[derive(Serialize)]
struct Token {
    version: Version,
    environment: String,
    stream: Stream,
    method: &'static str,
    path: String,
    issued_at_ms: DecimalU64,
    nonce: String,
    member_list_revision: DecimalU64,
    node_public_key: Digest,
}
