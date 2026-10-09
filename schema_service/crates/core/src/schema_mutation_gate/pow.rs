//! Payload hashing, proof-of-work checks and difficulty computation.

use super::*;

pub fn schema_payload_hash(
    payload: &Value,
) -> Result<String, app_identity_crypto::CanonicalizeError> {
    canonicalize(payload).map(|bytes| schema_types::hex::sha256_hex(&bytes))
}

pub fn node_signature_payload(
    schema_hash: &str,
    node_public_key_hash: &str,
    challenge_id: &str,
    nonce: &str,
    counter: u64,
) -> Value {
    json!({
        "purpose": "schema_mutation_node_publish",
        "schema_hash": schema_hash,
        "node_public_key_hash": node_public_key_hash,
        "challenge_id": challenge_id,
        "nonce": nonce,
        "counter": counter,
    })
}

pub fn pow_satisfies(
    nonce: &str,
    node_public_key_hash: &str,
    schema_hash: &str,
    counter: u64,
    difficulty_bits: u8,
) -> bool {
    let input = pow_input(nonce, node_public_key_hash, schema_hash, counter);
    let digest = Sha256::digest(input.as_bytes());
    leading_zero_bits(&digest) >= difficulty_bits as usize
}

pub fn pow_input(
    nonce: &str,
    node_public_key_hash: &str,
    schema_hash: &str,
    counter: u64,
) -> String {
    format!("{nonce}:{node_public_key_hash}:{schema_hash}:{counter}")
}

pub(super) fn adaptive_difficulty_bits(
    cfg: &SchemaMutationGateConfig,
    store: &SchemaMutationGateStore,
    node_public_key_hash: &str,
    remote_ip: Option<&str>,
    app_id: Option<&str>,
    dev_pubkey: Option<&str>,
    now: u64,
) -> Result<u8, SchemaMutationGateError> {
    let mut recent = bucket_len(store, cfg, &format!("node:{node_public_key_hash}"), now)?;
    if let Some(ip) = remote_ip.and_then(normalize_ip_bucket) {
        recent += bucket_len(store, cfg, &format!("ip:{ip}"), now)?;
    }
    if let Some(app_id) = app_id.filter(|s| !s.is_empty()) {
        recent += bucket_len(store, cfg, &format!("app:{app_id}"), now)?;
    }
    if let Some(dev_pubkey) = dev_pubkey.filter(|s| !s.is_empty()) {
        recent += bucket_len(
            store,
            cfg,
            &format!(
                "dev:{}",
                schema_types::hex::sha256_hex(dev_pubkey.as_bytes())
            ),
            now,
        )?;
    }
    Ok((recent / 10).min(cfg.max_adaptive_difficulty_bits as usize) as u8)
}

pub fn catalog_size_difficulty_bits(user_schema_count: usize, max_bits: u8) -> u8 {
    if user_schema_count < 64 {
        return 0;
    }
    ((usize::BITS - (user_schema_count / 64).leading_zeros() - 1) as u8).min(max_bits)
}

/// Maximum PoW difficulty whose *expected* grind work fits inside `ttl` for a
/// conservative single-thread client. Uncapped base+adaptive+catalog totals
/// can otherwise exceed the challenge TTL under load (adaptive) or on large
/// catalogs, so legitimate clients only observe `proof_of_work_expired`.
pub fn max_difficulty_bits_for_challenge_ttl(ttl: Duration) -> u8 {
    let ttl_secs = ttl.as_secs().max(1);
    let budget_secs = (ttl_secs.saturating_mul(CHALLENGE_SOLVE_BUDGET_TTL_NUM)
        / CHALLENGE_SOLVE_BUDGET_TTL_DEN)
        .max(1);
    let max_hashes = CONSERVATIVE_CLIENT_HASHES_PER_SEC.saturating_mul(budget_secs);
    if max_hashes <= 1 {
        return 0;
    }
    // floor(log2(max_hashes))
    (63 - max_hashes.leading_zeros()) as u8
}
