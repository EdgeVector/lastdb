//! Materialize a verified history into a real FoldDB and check what it holds.

use std::path::Path;

use mutation_log::VerifiedHistory;
use rand::RngCore;
use serde_json::json;
use signed_history_materialize::{
    apply_history, decode_string_field, ensure_hash_schema, open_proof_store, read_field_map,
    FoldDB, NoFiles,
};

/// Namespace (FoldDB schema name) every proof scenario writes to.
pub const NAMESPACE: &str = "demo";

/// One record the store must hold after replay: key and its `v` string.
pub struct Expected<'a> {
    pub record_key: &'a str,
    pub value: &'a str,
}

/// Open a fresh proof store in `dir`, replay `history` into it twice (the
/// second pass must change nothing), and check every expected record by keyed
/// read. Also checks the stored value equals what the history's winner decodes to.
pub async fn replay_and_check(
    dir: &Path,
    history: &VerifiedHistory,
    expected: &[Expected<'_>],
) -> Result<(), String> {
    let mut seed = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut seed);
    let store = open_proof_store(dir, &seed).await?;
    let checked = check(&store, history, expected).await;
    let closed = store.shutdown().await.map_err(|e| e.to_string());
    checked?;
    closed
}

async fn check(
    store: &FoldDB,
    history: &VerifiedHistory,
    expected: &[Expected<'_>],
) -> Result<(), String> {
    ensure_hash_schema(store, NAMESPACE).await?;
    apply_history(store, history, &NoFiles).await?;
    apply_history(store, history, &NoFiles).await?;
    for want in expected {
        let fields = read_field_map(store, NAMESPACE, want.record_key)
            .await?
            .ok_or_else(|| format!("FoldDB has no record {NAMESPACE}/{}", want.record_key))?;
        let expected_fields: std::collections::HashMap<String, serde_json::Value> =
            std::iter::once(("v".to_owned(), json!(want.value))).collect();
        if fields != expected_fields {
            return Err(format!(
                "FoldDB {NAMESPACE}/{} holds {fields:?}, expected {expected_fields:?}",
                want.record_key
            ));
        }
        let winner = history
            .records()
            .get(&(NAMESPACE.to_owned(), want.record_key.to_owned()))
            .ok_or_else(|| format!("history has no winner for {}", want.record_key))?;
        if decode_string_field(&winner.value)? != want.value {
            return Err(format!("history winner for {} differs", want.record_key));
        }
    }
    Ok(())
}
