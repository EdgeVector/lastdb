//! Node-signed POST authorization for share HTTP paths.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_protocol::v2::share_access::{
    NodeReadToken, ShareHttpMethod, SHARE_NODE_AUTHORIZATION_SCHEME,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, Id, Text};
use fold_db::security::Ed25519KeyPair;
use rand::RngCore;

pub(crate) use cloud_backup_protocol::v2::share_access::ShareEnvironment;

/// Account, database, realm, and raw path of one signed POST.
#[derive(Clone, Copy)]
pub(crate) struct PathPostTarget<'a> {
    pub scope: &'a CloudScope,
    pub database_id: &'a Id,
    pub environment: ShareEnvironment,
    pub path: &'a str,
}

/// Authorization header value for one POST. A new nonce is used every call.
pub(crate) fn authorization_post(
    keypair: &Ed25519KeyPair,
    target: PathPostTarget<'_>,
    body: &[u8],
    now_ms: u64,
) -> std::result::Result<String, String> {
    if now_ms == 0 {
        return Err("node read is not bound to a time".into());
    }
    let mut nonce_bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    let node_public_key = Digest::try_from(fold_db::hex::hex_lower(keypair.public_key_bytes()))
        .map_err(|_| "node key was refused")?;
    let token = NodeReadToken {
        version: Version::V2,
        environment: target.environment,
        database_id: target.database_id.clone(),
        stream: Stream {
            scope: target.scope.clone(),
            stream_id: target.database_id.clone(),
        },
        method: ShareHttpMethod::Post,
        path: Text::try_from(target.path.to_owned()).map_err(|_| "node read path was refused")?,
        issued_at_ms: DecimalU64::new(now_ms),
        nonce: Text::try_from(fold_db::hex::hex_lower(nonce_bytes))
            .map_err(|_| "node read nonce was refused")?,
        node_public_key,
    };
    let digest = token
        .digest(body)
        .map_err(|_| "node read digest was refused")?;
    let signature = keypair.sign(digest.as_str().as_bytes());
    let token_json = serde_json::to_vec(&token).map_err(|_| "node read token was refused")?;
    let encoded_token = URL_SAFE_NO_PAD.encode(token_json);
    let encoded_signature = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    Ok(format!(
        "{SHARE_NODE_AUTHORIZATION_SCHEME} {encoded_token}.{encoded_signature}"
    ))
}
