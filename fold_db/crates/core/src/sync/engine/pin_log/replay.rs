use super::*;

/// Decrypt, verify hash, deserialize — inverse of [`seal_mutation_log_segment`].
///
/// Exists so the sealed bytes are provably openable rather than write-only
/// ciphertext. `PinLogRecord` had no cloud reader at all when the plaintext
/// defect was found, which is a large part of why nobody noticed the writer
/// was emitting bare JSON: nothing ever tried to open what it wrote.
///
/// `transfer/s3_io/fetch.rs` calls this on the production replay path to
/// recognize a mutation-log segment sharing the legacy flat `log/{seq}.enc`
/// namespace with replayable `LogEntry` objects.
pub async fn unseal_mutation_log_segment(
    sealed: &[u8],
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<Vec<PinLogRecord>, String> {
    let json = open_mutation_log_json(sealed, crypto).await?;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum WirePayload {
        Batch(Vec<PinLogRecord>),
        LegacySingle(Box<PinLogRecord>),
    }

    let records = match serde_json::from_slice(&json)
        .map_err(|e| format!("decode mutation log segment: {e}"))?
    {
        WirePayload::Batch(records) => records,
        WirePayload::LegacySingle(record) => vec![*record],
    };
    if records.is_empty() {
        return Err("mutation log segment decoded to an empty record batch".to_string());
    }
    Ok(records)
}

mod apply;
mod groups;
mod object_keys;
mod retired_verify;
mod sources;
mod staging;

pub use apply::*;
use groups::*;
pub(crate) use object_keys::*;
pub use retired_verify::verify_retired_replay_source;
use staging::*;
