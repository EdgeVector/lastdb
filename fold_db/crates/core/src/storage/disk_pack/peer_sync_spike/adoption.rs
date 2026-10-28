//! Stage an encrypted native generation, replay original tails, then select it once.

use super::keys::{event_key, STATE_KEY};
use super::model::State;
use super::snapshot::{copy_namespace, open_snapshot, required_state};
use super::*;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdoptionReport {
    pub generation: String,
    pub retained_entries: usize,
    pub frontiers: BTreeMap<String, Position>,
}

impl Replica {
    /// No old generation is removed. This prototype excludes concurrent root commands.
    pub async fn adopt(&mut self, input: &Path) -> StorageResult<AdoptionReport> {
        let control = Arc::clone(
            self.control
                .as_ref()
                .ok_or_else(|| invalid("adoption needs an owned prototype root"))?,
        );
        control.require_ready()?;
        let gate = self.runtime.enter_write().await;
        let local = required_state(self).await?;
        let (snapshot, info) = open_snapshot(input, &control.key).await?;
        let incoming = required_state(&snapshot).await?;
        check_lineages(self, &snapshot, &local, &incoming).await?;
        let tails = uncovered_entries(self, &local, &incoming).await?;
        super::stream::validate_import(&tails)?;
        let generation = control.new_generation()?;
        let mut candidate =
            Self::open_generation(&control.generation(&generation)?, &control.key).await?;
        let copied = copy_namespace(&snapshot, &candidate, &incoming).await?;
        if copied != info {
            return Err(invalid("adoption source changed after validation"));
        }
        let imported = candidate.import(&tails).await?;
        if imported.accepted != tails.len() || imported.duplicates != 0 {
            return Err(invalid(
                "adoption did not retain every uncovered original entry",
            ));
        }
        let mut final_state = required_state(&candidate).await?;
        final_state
            .clock
            .observe_remote(local.clock.physical_nanos, local.clock.logical_counter);
        preserve_frontiers(&local, &final_state)?;
        candidate.kv.put(STATE_KEY, encode(&final_state)?).await?;
        candidate.kv.flush().await?;
        control.select(&generation)?;
        candidate.control = Some(control);
        let report = AdoptionReport {
            generation,
            retained_entries: tails.len(),
            frontiers: final_state.frontiers,
        };
        drop(gate);
        *self = candidate;
        Ok(report)
    }
}

async fn check_lineages(
    local: &Replica,
    incoming: &Replica,
    before: &State,
    snapshot: &State,
) -> StorageResult<()> {
    let mut local_proofs = Vec::new();
    let mut incoming_proofs = Vec::new();
    for (origin, old) in &before.frontiers {
        let Some(new) = snapshot.frontiers.get(origin) else {
            continue;
        };
        if old.sequence == new.sequence {
            if old.sha256 != new.sha256 {
                return Err(invalid("snapshot forks an existing origin"));
            }
        } else if old.sequence > new.sequence {
            local_proofs.push((origin.clone(), new.clone()));
        } else {
            incoming_proofs.push((origin.clone(), old.clone()));
        }
    }
    // Each side supplies all requested proofs in one native batch read.
    futures::try_join!(
        check_proof_batches(local, &local_proofs),
        check_proof_batches(incoming, &incoming_proofs),
    )?;
    Ok(())
}

async fn check_proof_batches(
    replica: &Replica,
    proofs: &[(String, Position)],
) -> StorageResult<()> {
    for chunk in proofs.chunks(MAX_EXPORT_BATCH) {
        let keys = chunk
            .iter()
            .map(|(origin, position)| event_key(origin, position.sequence))
            .collect();
        check_proofs(chunk, replica.kv.get_many(keys).await?)?;
    }
    Ok(())
}

fn check_proofs(proofs: &[(String, Position)], values: Vec<Option<Vec<u8>>>) -> StorageResult<()> {
    if proofs.len() != values.len() {
        return Err(invalid("snapshot proof batch is incomplete"));
    }
    for ((origin, position), bytes) in proofs.iter().zip(values) {
        let entry: Entry =
            decode(&bytes.ok_or_else(|| {
                invalid("snapshot ancestry needs a missing retained prefix proof")
            })?)?;
        entry.verify()?;
        if entry.origin != *origin || entry.position() != *position {
            return Err(invalid("snapshot origin ancestry differs"));
        }
    }
    Ok(())
}

async fn uncovered_entries(
    replica: &Replica,
    local: &State,
    snapshot: &State,
) -> StorageResult<Vec<Entry>> {
    let mut wanted = Vec::new();
    for (origin, head) in &local.frontiers {
        let after = snapshot
            .frontiers
            .get(origin)
            .map_or(0, |position| position.sequence);
        let count = head.sequence.saturating_sub(after);
        if count > MAX_PAGE as u64 || wanted.len().saturating_add(count as usize) > MAX_PAGE {
            return Err(invalid(
                "adoption has more than 128 uncovered original entries",
            ));
        }
        for offset in 1..=count {
            wanted.push((origin.clone(), after + offset));
        }
    }
    let mut entries = Vec::with_capacity(wanted.len());
    let mut bytes = 2_usize;
    let mut changes = 0_usize;
    for chunk in wanted.chunks(MAX_EXPORT_BATCH) {
        let keys = chunk
            .iter()
            .map(|(origin, sequence)| event_key(origin, *sequence))
            .collect();
        let values = replica.kv.get_many(keys).await?;
        if values.len() != chunk.len() {
            return Err(invalid("retained tail batch is incomplete"));
        }
        for ((origin, sequence), value) in chunk.iter().zip(values) {
            let entry = decode_tail(origin, *sequence, value)?;
            bytes += encode(&entry)?.len() + usize::from(!entries.is_empty());
            changes += entry.changes.len();
            if bytes > MAX_IMPORT_BYTES || changes > MAX_IMPORT_CHANGES {
                return Err(invalid("adoption tail exceeds 1 MiB or 1024 changes"));
            }
            entries.push(entry);
        }
    }
    Ok(entries)
}

fn decode_tail(origin: &str, sequence: u64, bytes: Option<Vec<u8>>) -> StorageResult<Entry> {
    let entry: Entry = decode(&bytes.ok_or_else(|| {
        invalid("adoption requires an unavailable original tail; old generation stays active")
    })?)?;
    entry.verify()?;
    if entry.origin != origin || entry.sequence != sequence {
        return Err(invalid(
            "retained tail key differs from its signed identity",
        ));
    }
    Ok(entry)
}

fn preserve_frontiers(local: &State, candidate: &State) -> StorageResult<()> {
    if candidate.frontiers.len() > MAX_ORIGINS {
        return Err(invalid("adoption exceeds 128 origins"));
    }
    for (origin, before) in &local.frontiers {
        let after = candidate
            .frontiers
            .get(origin)
            .ok_or_else(|| invalid("adoption lost an origin"))?;
        if after.sequence < before.sequence
            || (after.sequence == before.sequence && after.sha256 != before.sha256)
        {
            return Err(invalid(
                "adoption lost or forked a retained origin frontier",
            ));
        }
    }
    Ok(())
}
