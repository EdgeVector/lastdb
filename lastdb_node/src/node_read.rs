//! `LastDBNode` authorization for one mutation-log body.
//!
//! H verifies [`NodeReadToken::digest`] of the exact HTTP body. The token JSON
//! includes `database_id`. E's private token omits that field, so H treats it
//! as a legacy proof. Cutover calls this helper. It is the same in `dev` and
//! in `prod`.

use base64::Engine as _;
use cloud_backup_protocol::v2::share_access::{
    NodeReadToken, ShareEnvironment, ShareHttpMethod, SHARE_NODE_AUTHORIZATION_SCHEME,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, Id, Text};
use fold_db::security::Ed25519KeyPair;
use rand::RngCore;

/// Authorization header value for one POST. A new nonce is used every call.
///
/// `body` is the exact octets that will be sent. `path` includes `/api`.
pub fn authorization(
    keypair: &Ed25519KeyPair,
    scope: &CloudScope,
    database_id: &Id,
    environment: ShareEnvironment,
    path: &str,
    body: &[u8],
    member_revision: u64,
    now_ms: u64,
) -> Result<String, String> {
    let token = token(
        keypair,
        scope,
        database_id,
        environment,
        path,
        member_revision,
        now_ms,
    )?;
    let digest = token
        .digest(body)
        .map_err(|_| "node read digest was refused")?;
    let signature = keypair.sign(digest.as_str().as_bytes());
    let token_json = serde_json::to_vec(&token).map_err(|_| "node read token was refused")?;
    let encoded_token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_json);
    let encoded_signature =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature.to_bytes());
    Ok(format!(
        "{SHARE_NODE_AUTHORIZATION_SCHEME} {encoded_token}.{encoded_signature}"
    ))
}

fn token(
    keypair: &Ed25519KeyPair,
    scope: &CloudScope,
    database_id: &Id,
    environment: ShareEnvironment,
    path: &str,
    member_revision: u64,
    now_ms: u64,
) -> Result<NodeReadToken, String> {
    if now_ms == 0 {
        return Err("node read is not bound to a time".into());
    }
    let mut nonce_bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    let node_public_key = Digest::try_from(fold_db::hex::hex_lower(keypair.public_key_bytes()))
        .map_err(|_| "node key was refused")?;
    Ok(NodeReadToken {
        version: Version::V2,
        environment,
        database_id: database_id.clone(),
        stream: Stream {
            scope: scope.clone(),
            stream_id: database_id.clone(),
        },
        method: ShareHttpMethod::Post,
        path: Text::try_from(String::from(path)).map_err(|_| "node read path was refused")?,
        issued_at_ms: DecimalU64::new(now_ms),
        nonce: Text::try_from(fold_db::hex::hex_lower(nonce_bytes))
            .map_err(|_| "node read nonce was refused")?,
        member_list_revision: DecimalU64::new(member_revision),
        node_public_key,
    })
}
