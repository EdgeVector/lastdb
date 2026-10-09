//! Challenge MAC issue/verify and encoding helpers.

use super::*;

pub(super) fn decode_b64_json<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, ()> {
    let bytes = BASE64.decode(value.trim()).map_err(|_| ())?;
    serde_json::from_slice(&bytes).map_err(|_| ())
}

pub(super) fn is_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

pub(super) fn issue_challenge_mac(
    cfg: &SchemaMutationGateConfig,
    nonce: &str,
    node_public_key_hash: &str,
    schema_hash: &str,
    difficulty_bits: u8,
    expires_at_unix_secs: u64,
) -> Result<String, SchemaMutationGateError> {
    let mut mac = HmacSha256::new_from_slice(&cfg.hmac_secret)
        .map_err(|e| SchemaMutationGateError::Internal(e.to_string()))?;
    mac.update(
        challenge_mac_input(
            nonce,
            node_public_key_hash,
            schema_hash,
            difficulty_bits,
            expires_at_unix_secs,
        )
        .as_bytes(),
    );
    Ok(BASE64.encode(mac.finalize().into_bytes()))
}

pub(super) fn verify_challenge_mac(
    cfg: &SchemaMutationGateConfig,
    nonce: &str,
    node_public_key_hash: &str,
    schema_hash: &str,
    difficulty_bits: u8,
    expires_at_unix_secs: u64,
    challenge_mac: &str,
) -> Result<(), SchemaMutationGateError> {
    let expected = issue_challenge_mac(
        cfg,
        nonce,
        node_public_key_hash,
        schema_hash,
        difficulty_bits,
        expires_at_unix_secs,
    )?;
    let expected = BASE64
        .decode(expected)
        .map_err(|_| SchemaMutationGateError::Internal("invalid issued mac".to_string()))?;
    let provided = BASE64
        .decode(challenge_mac.trim())
        .map_err(|_| SchemaMutationGateError::ProofOfWorkInvalid)?;
    let mut mac = HmacSha256::new_from_slice(&cfg.hmac_secret)
        .map_err(|e| SchemaMutationGateError::Internal(e.to_string()))?;
    mac.update(
        challenge_mac_input(
            nonce,
            node_public_key_hash,
            schema_hash,
            difficulty_bits,
            expires_at_unix_secs,
        )
        .as_bytes(),
    );
    mac.verify_slice(&provided)
        .map_err(|_| SchemaMutationGateError::ProofOfWorkInvalid)?;
    if provided != expected {
        return Err(SchemaMutationGateError::ProofOfWorkInvalid);
    }
    Ok(())
}

pub(super) fn challenge_mac_input(
    nonce: &str,
    node_public_key_hash: &str,
    schema_hash: &str,
    difficulty_bits: u8,
    expires_at_unix_secs: u64,
) -> String {
    format!("{nonce}:{node_public_key_hash}:{schema_hash}:{difficulty_bits}:{expires_at_unix_secs}")
}

pub(super) fn leading_zero_bits(bytes: &[u8]) -> usize {
    let mut count = 0;
    for byte in bytes {
        if *byte == 0 {
            count += 8;
        } else {
            count += byte.leading_zeros() as usize;
            break;
        }
    }
    count
}
