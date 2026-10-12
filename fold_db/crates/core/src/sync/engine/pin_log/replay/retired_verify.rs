//! Reuse normal object authentication and group assembly for exact recovery.

use super::*;

pub async fn verify_retired_replay_source(
    source: &RetiredReplaySource,
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<(), String> {
    let segments: Vec<_> = source
        .objects
        .iter()
        .map(|object| MutationLogSegment {
            segment: object.segment.clone(),
            payload: object.ciphertext.to_vec(),
        })
        .collect();
    let mut units = open_mutation_log_replay_units(&segments, crypto).await?;
    if units.len() != 1 {
        return Err("retired source must reconstruct one complete replay unit".into());
    }
    let unit = units.pop().ok_or("retired source replay unit is absent")?;
    let keys: Vec<_> = source
        .objects
        .iter()
        .map(|object| object.segment.object_key.clone())
        .collect();
    if unit.transaction_group != source.transaction_group || unit.source_object_keys != keys {
        return Err("retired source object context differs".into());
    }
    let validated = apply::validate_replay_units(vec![unit])
        .map_err(|_| "retired source native record validation failed")?;
    let record = validated[0]
        .records
        .get(source.record_index)
        .ok_or("retired source original record position is absent")?;
    let actual = serde_json::to_value(record).map_err(|_| "retired source record decode failed")?;
    let expected =
        serde_json::to_value(&source.record).map_err(|_| "retired source record decode failed")?;
    if actual != expected {
        return Err("retired source original record differs".into());
    }
    Ok(())
}
