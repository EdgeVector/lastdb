use super::keys::{data_key, event_key, STATE_KEY};
use super::model::{State, Winner};
use super::*;
use crate::storage::traits::KvMutation;
use std::collections::BTreeMap;

pub(super) fn validate_import(entries: &[Entry]) -> StorageResult<()> {
    if entries.len() > MAX_PAGE {
        return Err(invalid("import exceeds 128 entries"));
    }
    let mut bytes = 2 + entries.len().saturating_sub(1);
    let mut changes = 0usize;
    for entry in entries {
        entry.verify()?;
        bytes = bytes.saturating_add(encode(entry)?.len());
        changes = changes.saturating_add(entry.changes.len());
    }
    if bytes > MAX_IMPORT_BYTES || changes > MAX_IMPORT_CHANGES {
        return Err(invalid("import exceeds 1 MiB or 1024 changes"));
    }
    Ok(())
}

impl Replica {
    pub async fn export(
        &self,
        origin: &str,
        after: u64,
        limit: usize,
    ) -> StorageResult<Vec<Entry>> {
        if limit == 0 || limit > MAX_PAGE || origin.len() > 128 {
            return Err(invalid("export needs one origin and 1..=128 entries"));
        }
        let state = self.state().await?;
        let high = state
            .frontiers
            .get(origin)
            .map_or(0, |position| position.sequence);
        if after >= high {
            return Ok(Vec::new());
        }
        let count = (high - after).min(limit.min(MAX_EXPORT_BATCH) as u64);
        let keys = (1..=count)
            .map(|offset| event_key(origin, after + offset))
            .collect();
        bounded_export(self.kv.get_many(keys).await?)
    }

    pub(super) async fn stage_entries(
        &self,
        entries: &[Entry],
        mut state: State,
    ) -> StorageResult<ImportReport> {
        let keys: Vec<_> = entries
            .iter()
            .map(|entry| event_key(&entry.origin, entry.sequence))
            .collect();
        let mut known: BTreeMap<_, _> = keys
            .iter()
            .cloned()
            .zip(self.kv.get_many(keys.clone()).await?)
            .collect();
        let mut report = ImportReport::default();
        let mut accepted = Vec::new();
        for entry in entries {
            let position = state
                .frontiers
                .get(&entry.origin)
                .cloned()
                .unwrap_or_default();
            let key = event_key(&entry.origin, entry.sequence);
            if entry.sequence <= position.sequence {
                require_duplicate(entry, &position, known.get(&key).and_then(Option::as_deref))?;
                report.duplicates += 1;
                continue;
            }
            if position.sequence.checked_add(1) != Some(entry.sequence)
                || position.sha256 != entry.previous_sha256
            {
                return Err(invalid("peer entry has a gap or a different hash ancestry"));
            }
            if !state.frontiers.contains_key(&entry.origin) && state.frontiers.len() == MAX_ORIGINS
            {
                return Err(invalid("origin map exceeds 128 origins"));
            }
            let bytes = encode(entry)?;
            known.insert(key, Some(bytes));
            state
                .frontiers
                .insert(entry.origin.clone(), entry.position());
            state
                .clock
                .observe_remote(entry.last_written_at()?, entry.logical_counter);
            accepted.push(entry);
            report.accepted += 1;
        }
        if accepted.is_empty() {
            return Ok(report);
        }
        let (mut operations, changed) = self.winner_operations(&accepted).await?;
        report.changed = changed;
        for entry in accepted {
            operations.push(KvMutation::put(
                event_key(&entry.origin, entry.sequence),
                encode(entry)?,
            ));
        }
        operations.push(KvMutation::put(STATE_KEY.to_vec(), encode(&state)?));
        self.kv.batch_mutate(operations).await?;
        Ok(report)
    }

    async fn winner_operations(
        &self,
        entries: &[&Entry],
    ) -> StorageResult<(Vec<KvMutation>, usize)> {
        let mut winners: BTreeMap<Vec<u8>, Option<Winner>> = entries
            .iter()
            .flat_map(|entry| entry.changes.iter())
            .map(|change| (data_key(&change.hash, &change.range), None))
            .collect();
        let keys = winners.keys().cloned().collect();
        for (winner, bytes) in winners.values_mut().zip(self.kv.get_many(keys).await?) {
            *winner = bytes.map(|bytes| decode(&bytes)).transpose()?;
        }
        let mut changed = BTreeMap::new();
        for entry in entries {
            for index in 0..entry.changes.len() {
                let incoming = Winner::from_entry(entry, index);
                let key = data_key(&incoming.change.hash, &incoming.change.range);
                let current = winners
                    .get_mut(&key)
                    .expect("all incoming keys were collected");
                if current.as_ref().is_none_or(|local| incoming.wins(local)) {
                    changed.insert(key, encode(&incoming)?);
                    *current = Some(incoming);
                }
            }
        }
        let count = changed.len();
        let operations = changed
            .into_iter()
            .map(|(key, value)| KvMutation::put(key, value))
            .collect();
        Ok((operations, count))
    }
}

fn bounded_export(rows: Vec<Option<Vec<u8>>>) -> StorageResult<Vec<Entry>> {
    let mut entries = Vec::new();
    let mut bytes = 2usize;
    let mut changes = 0usize;
    for row in rows {
        let row = row.ok_or_else(|| {
            invalid("covered origin prefix is unavailable; a snapshot checkpoint is required")
        })?;
        let entry: Entry = decode(&row)?;
        entry.verify()?;
        let next_bytes = bytes + usize::from(!entries.is_empty()) + row.len();
        let next_changes = changes + entry.changes.len();
        if next_bytes > MAX_IMPORT_BYTES || next_changes > MAX_IMPORT_CHANGES {
            break;
        }
        bytes = next_bytes;
        changes = next_changes;
        entries.push(entry);
    }
    Ok(entries)
}

fn require_duplicate(
    entry: &Entry,
    position: &Position,
    bytes: Option<&[u8]>,
) -> StorageResult<()> {
    if let Some(bytes) = bytes {
        let previous: Entry = decode(bytes)?;
        if previous.sha256 == entry.sha256 {
            return Ok(());
        }
    } else if entry.sequence == position.sequence && entry.sha256 == position.sha256 {
        return Ok(());
    }
    Err(invalid(
        "peer entry forks or lacks a retained duplicate receipt",
    ))
}
