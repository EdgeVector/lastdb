use super::*;

pub(super) async fn open_mutation_log_json(
    sealed: &[u8],
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<Vec<u8>, String> {
    let plaintext = crypto
        .decrypt(sealed)
        .await
        .map_err(|e| format!("unseal mutation log segment: {e}"))?;
    if plaintext.len() < MUTATION_LOG_SEGMENT_HASH_SIZE {
        return Err(format!(
            "mutation log segment too short to carry its hash: {} bytes",
            plaintext.len()
        ));
    }
    let (hash, json) = plaintext.split_at(MUTATION_LOG_SEGMENT_HASH_SIZE);
    let actual = Sha256::digest(json);
    if actual.as_slice() != hash {
        return Err("mutation log segment hash mismatch after decrypt".to_string());
    }
    Ok(json.to_vec())
}
