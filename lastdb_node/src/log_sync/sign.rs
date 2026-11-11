//! `LastDBNode` authorization. The digest is `NodeReadProof::digest` of the raw body.

use base64::Engine as _;
use cloud_backup_protocol::v2::share_access::{
    Ed25519Signature, NodeReadProof, NodeReadToken, ShareEnvironment, ShareHttpMethod,
    SHARE_NODE_AUTHORIZATION_SCHEME,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, Id, ProtocolError, Text};
use fold_db::security::Ed25519KeyPair;
use rand::RngCore;

/// Authorization header value for one POST. A new nonce is used every call.
pub(crate) fn authorization(
    keypair: &Ed25519KeyPair,
    scope: &CloudScope,
    database_id: &Id,
    path: &str,
    body: &[u8],
    member_revision: u64,
    now_ms: u64,
) -> Result<String, ProtocolError> {
    if now_ms == 0 {
        return Err(ProtocolError::invalid("share.node_read.issued_at_ms"));
    }
    let token = token(keypair, scope, database_id, path, member_revision, now_ms)?;
    let digest = token.digest(body)?;
    let signature = keypair.sign(digest.as_str().as_bytes());
    let proof = NodeReadProof {
        token,
        signature: Ed25519Signature::try_from(hex::encode(signature.to_bytes()))?,
    };
    let confirmed = proof.digest(body)?;
    if confirmed != digest {
        return Err(ProtocolError::invalid("share.node_read.digest"));
    }
    let token_json = proof.token_json()?;
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
    path: &str,
    member_revision: u64,
    now_ms: u64,
) -> Result<NodeReadToken, ProtocolError> {
    let mut nonce_bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    Ok(NodeReadToken {
        version: Version::V2,
        environment: ShareEnvironment::Dev,
        database_id: database_id.clone(),
        stream: Stream {
            scope: scope.clone(),
            stream_id: database_id.clone(),
        },
        method: ShareHttpMethod::Post,
        path: Text::try_from(path.to_owned())?,
        issued_at_ms: DecimalU64::new(now_ms),
        nonce: Text::try_from(hex::encode(nonce_bytes))?,
        member_list_revision: DecimalU64::new(member_revision),
        node_public_key: Digest::try_from(hex::encode(keypair.public_key_bytes()))?,
    })
}
