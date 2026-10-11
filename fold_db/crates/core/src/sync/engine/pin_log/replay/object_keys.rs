use super::*;

pub(crate) use super::super::helpers::parse_mutation_log_object_key;

pub(in super::super) fn mutation_log_segment_id_from_object_key(
    listed_object_key: &str,
    through_id: u64,
) -> Result<MutationLogSegmentId, String> {
    let relative = super::super::helpers::relative_mutation_log_key(listed_object_key)
        .unwrap_or(listed_object_key);
    let body = relative
        .strip_prefix("log/")
        .ok_or_else(|| format!("mutation-log object key is outside log/: {listed_object_key}"))?;
    let parts = body.split('/').collect::<Vec<_>>();
    let segment = if let [writer, filename] = parts.as_slice() {
        let sequence = filename
            .strip_suffix(".enc")
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| format!("invalid mutation-log object key: {listed_object_key}"))?;
        MutationLogSegmentId {
            writer_id: Some((*writer).to_string()),
            schema_name: None,
            utc_nanos: None,
            sequence: None,
            through_id: sequence,
            object_key: MutationLogSegmentId::default_object_key(Some(writer), sequence),
        }
    } else if parts.len() >= 3 {
        let writer = parts[0];
        let filename = parts[parts.len() - 1];
        let schema_parts = &parts[1..parts.len() - 1];
        if writer.is_empty()
            || schema_parts
                .iter()
                .any(|component| component.is_empty() || component.contains(".."))
        {
            return Err(format!(
                "invalid mutation-log object key shape: {listed_object_key}"
            ));
        }
        let schema = schema_parts.join("/");
        let stem = filename
            .strip_suffix(".enc")
            .ok_or_else(|| format!("invalid mutation-log object key: {listed_object_key}"))?;
        let (utc_nanos, sequence) = stem
            .rsplit_once('_')
            .and_then(|(utc_nanos, sequence)| {
                Some((
                    utc_nanos.parse::<u64>().ok()?,
                    sequence.parse::<u64>().ok()?,
                ))
            })
            .ok_or_else(|| format!("invalid mutation-log object key: {listed_object_key}"))?;
        MutationLogSegmentId::schema_folder(writer, schema, utc_nanos, sequence, sequence)
    } else {
        return Err(format!(
            "invalid mutation-log object key shape: {listed_object_key}"
        ));
    };
    if segment.through_id != through_id || segment.object_key != relative {
        return Err(format!(
            "mutation-log object key {listed_object_key} does not match through-id {through_id}"
        ));
    }
    Ok(segment)
}

/// Pure helper: pick the writer-scoped mutation-log segment objects one
/// peer-apply / restore cycle must actually download.
///
/// Returns `(candidates, flat_classic_skipped)`, where each candidate is
/// `(writer_id, through_id, listed_object_key)`, sorted by writer then
/// through_id.
///
/// Only `log/{writer}/{seq}.enc` keys name a mutation-log segment. Every
/// segment upload keys itself with `default_object_key(Some(writer), ..)`, so
/// the flat `log/{seq}.enc` shape -- which `parse_mutation_log_object_key`
/// also accepts -- is always classic `LogEntry` ciphertext owned by the
/// ordinary download cursor, never a segment.
///
/// Dropping flat keys *before* the presign+download fan-out is the fix for a
/// cycle-time defect. They used to reach the fan-out and were rejected only
/// afterwards, when `unseal_mutation_log_segment` failed and the loop hit
/// `continue`, so every cycle re-downloaded the whole flat log prefix and threw
/// it away. Measured on the primary 2026-08-26 with `remote_log_entries=11358`:
/// one `do_sync` spent ~38 minutes inside this download for
/// `segments_considered=1`, so the configured 30s `sync_interval_ms` was never
/// observable and `rpo_secs` climbed at wall-clock rate. The returned segment
/// set is unchanged -- those objects could never unseal as segments -- only the
/// wasted downloads are gone.
pub(crate) fn select_peer_apply_candidates<'a, I>(
    listed_keys: I,
    incorporated_frontier: &Frontier,
) -> (Vec<(String, u64, String)>, u64)
where
    I: IntoIterator<Item = &'a str>,
{
    let mut candidates: Vec<(String, u64, String)> = Vec::new();
    let mut flat_classic_skipped: u64 = 0;
    for key in listed_keys {
        let Some((writer, through_id)) = parse_mutation_log_object_key(key) else {
            continue;
        };
        let Some(writer) = writer else {
            flat_classic_skipped = flat_classic_skipped.saturating_add(1);
            continue;
        };
        if incorporated_frontier.covers_log(Some(writer.as_str()), through_id) {
            continue;
        }
        candidates.push((writer, through_id, key.to_string()));
    }
    candidates.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    (candidates, flat_classic_skipped)
}

// lint:file-size-ok moved verbatim from pin_log.rs; cohesive unit, split further in a later pass
