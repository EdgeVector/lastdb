//! Restore: read streams from seq 1 via signed_history_client, AcceptEntry + LWW.

use app_identity_crypto::SigningKey;
use mutation_log::VerifiedHistory;
use signed_history_client::{restore_from_relay, RestoreRequest, StreamRef};

use super::materialize;

/// One owned device stream to restore (`device_id` is a UUID string).
#[derive(Clone, Debug)]
pub struct OwnedStream {
    /// UUID device id on the publisher/owner node key.
    pub device_id: String,
}

/// Parameters for restoring an owned database into a staged home.
pub struct RestoreOwnedParams<'a> {
    /// Staged home path (identity key already written).
    pub home: &'a std::path::Path,
    /// Relay origin URL.
    pub relay_url: String,
    /// Owner recovery seed (32 bytes) → SigningKey / node key.
    pub seed: &'a [u8; 32],
    /// Hex32 database id.
    pub database_id: String,
    /// Owner device streams to pull (writer_key = seed's public key).
    pub streams: Vec<OwnedStream>,
}

/// Counters returned after a successful restore.
#[derive(Clone, Debug)]
pub struct RestoreOwnedReport {
    /// Non-duplicate accepted entries.
    pub replayed: u64,
    /// Max writer_seq among accepted entries.
    pub through: u64,
}

/// Pull every owned stream from relay seq 1, AcceptEntry, materialize into `home`.
pub async fn restore_owned_database(
    params: RestoreOwnedParams<'_>,
) -> Result<RestoreOwnedReport, String> {
    let signing_key = SigningKey::from_bytes(params.seed);
    let node_key_hex = app_identity_crypto::hex_lower(&signing_key.verifying_key().to_bytes());
    let reader_device = uuid::Uuid::new_v4().to_string();
    let streams: Vec<StreamRef> = params
        .streams
        .into_iter()
        .map(|s| StreamRef {
            writer_key: node_key_hex.clone(),
            device_id: s.device_id,
        })
        .collect();
    let report = restore_from_relay(RestoreRequest {
        relay_url: params.relay_url,
        signing_key,
        node_key_hex,
        device_id: reader_device,
        database_id: params.database_id.clone(),
        streams,
    })
    .await?;
    publish_restored(params.home, params.seed, &report.history).await?;
    Ok(RestoreOwnedReport {
        replayed: report.replayed,
        through: report.through,
    })
}

async fn publish_restored(
    home: &std::path::Path,
    seed: &[u8; 32],
    history: &VerifiedHistory,
) -> Result<(), String> {
    // The staged home has no booted host yet, so open its personal store at
    // `home/data` and let the next boot find the materialized records.
    let store = signed_history_materialize::open_store(&home.join("data"), seed, true).await?;
    let applied = declare_and_apply(&store, history).await;
    let closed = store.shutdown().await.map_err(|e| e.to_string());
    applied?;
    closed
}

async fn declare_and_apply(
    store: &fold_db::fold_db_core::FoldDB,
    history: &VerifiedHistory,
) -> Result<(), String> {
    // DESIGN: Phase 1 history carries no schema entries, so a namespace with no
    // declared schema gets the one-field Hash schema (`v: String`) before replay.
    let namespaces: std::collections::BTreeSet<&str> = history
        .records()
        .keys()
        .map(|(namespace, _)| namespace.as_str())
        .collect();
    for namespace in namespaces {
        signed_history_materialize::ensure_hash_schema(store, namespace).await?;
    }
    materialize::apply_history_records(store, history).await
}
