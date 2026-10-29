//! Keep zstd only when its saved bytes justify a later decode.

use super::*;

const MIN_INPUT_BYTES: usize = 256;
const MIN_SAVED_BYTES: usize = 64;
const MIN_SAVED_PERCENT: usize = 10;

pub(super) fn stored_payload(payload: &[u8]) -> Result<(Vec<u8>, bool)> {
    if payload.len() < MIN_INPUT_BYTES || payload.len() > MAX_DECOMPRESSED_FRAME_BYTES {
        crate::commit_timing::count("frame_compression_skipped_size", 1);
        return Ok((payload.to_vec(), false));
    }
    let compressed = zstd::bulk::compress(payload, ZSTD_LEVEL)
        .map_err(|e| Error::Config(format!("frame zstd compression failed: {e}")))?;
    let stored_len = compressed.len().saturating_add(UNCOMPRESSED_LEN_SIZE);
    let saved = payload.len().saturating_sub(stored_len);
    if saved < MIN_SAVED_BYTES || saved.saturating_mul(100) < payload.len() * MIN_SAVED_PERCENT {
        crate::commit_timing::count("frame_compression_skipped_savings", 1);
        return Ok((payload.to_vec(), false));
    }
    let mut stored = Vec::with_capacity(stored_len);
    stored.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    stored.extend_from_slice(&compressed);
    Ok((stored, true))
}
