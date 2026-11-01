//! Release known native log files only after the exact accepted recovery point.
use super::*;

fn read_entry(log: &MutationLog, epoch: &Id, sequence: u64) -> StorageResult<LogEntry> {
    store::read(&store::entry(&log.root, epoch, sequence)?.join("entry.json"))?
        .ok_or_else(|| invalid("complete mutation log entry is absent"))
}

fn parts(directory: &std::path::Path, entry: &LogEntry) -> StorageResult<()> {
    for part in &entry.parts {
        let parent = directory.join(part.role.directory());
        store::remove_part(
            &parent.join(format!("{}.ldrun", part.run_id.as_str())),
            part.bytes,
        )?;
        if parent.try_exists()? {
            std::fs::File::open(&parent)?.sync_all()?;
        }
    }
    Ok(())
}

impl MutationLog {
    pub fn acknowledge(&self, position: &LogPosition) -> StorageResult<()> {
        let (released, end) = {
            let cache = self.lock()?;
            let index = cache
                .index
                .as_ref()
                .ok_or_else(|| invalid("mutation log owner is absent"))?;
            if !index.epochs.contains(&position.epoch) {
                return Err(invalid("mutation log acknowledged epoch"));
            }
            let mut state = self.state(&position.epoch)?;
            let mut cursor = state.released.clone();
            if position.sequence <= cursor.sequence {
                return if position == &cursor {
                    Ok(())
                } else {
                    Err(invalid("mutation log acknowledgement regressed"))
                };
            }
            if position.sequence > state.head.sequence
                || position.sequence - cursor.sequence > super::store::MAX_ENTRIES
            {
                return Err(invalid("mutation log acknowledgement bound"));
            }
            let released = cursor.clone();
            for sequence in cursor.sequence + 1..=position.sequence {
                cursor = read_entry(self, &position.epoch, sequence)?.position_after(&cursor)?;
            }
            if cursor != *position {
                return Err(invalid("mutation log exact acknowledgement"));
            }
            state.acknowledged = state.acknowledged.max(position.sequence);
            store::write(
                &store::epoch(&self.root, &position.epoch).join("state.json"),
                &state,
            )?;
            (released, cursor)
        };
        // Native writes use the active journal while old file removals run outside its lock.
        let mut bytes = 0u64;
        for sequence in released.sequence + 1..=end.sequence {
            let entry = read_entry(self, &end.epoch, sequence)?;
            parts(&store::entry(&self.root, &end.epoch, sequence)?, &entry)?;
            bytes = bytes
                .checked_add(entry.bytes()?)
                .ok_or_else(|| invalid("mutation log released size overflow"))?;
        }
        let _owner = self.lock()?;
        let mut state = self.state(&end.epoch)?;
        if state.released != released || state.acknowledged < end.sequence {
            return Err(invalid("mutation log release identity changed"));
        }
        state.retained_bytes = state
            .retained_bytes
            .checked_sub(bytes)
            .ok_or_else(|| invalid("mutation log retained release counter"))?;
        state.released = end;
        store::write(
            &store::epoch(&self.root, &state.anchor.epoch).join("state.json"),
            &state,
        )
    }

    /// A complete accepted snapshot covers every older epoch. Never delete the active epoch.
    pub fn snapshot_accepted(&self, anchor: &LogAnchor) -> StorageResult<()> {
        let previous = {
            let cache = self.lock()?;
            let index = cache
                .index
                .as_ref()
                .ok_or_else(|| invalid("mutation log snapshot owner"))?;
            let state = self.state(&anchor.epoch)?;
            if state.anchor != *anchor || index.active.as_ref() != Some(&anchor.epoch) {
                return Err(invalid("mutation log accepted snapshot anchor"));
            }
            index
                .epochs
                .iter()
                .filter(|id| *id != &anchor.epoch)
                .cloned()
                .collect::<Vec<_>>()
        };
        for epoch in previous {
            self.release_epoch(&epoch, anchor)?;
        }
        self.finish_cleanup()
    }

    fn release_epoch(&self, epoch: &Id, covering: &LogAnchor) -> StorageResult<()> {
        let state = self.state(epoch)?;
        if state.head.frontiers.main > covering.frontiers.main
            || state.head.frontiers.cas > covering.frontiers.cas
        {
            return Err(invalid("snapshot does not cover the old mutation log"));
        }
        for sequence in 1..=state.head.sequence {
            self.remove_entry(epoch, sequence, false)?;
        }
        if state.head.sequence < store::MAX_ENTRIES {
            self.remove_entry(epoch, state.head.sequence + 1, true)?;
        }
        // The durable index must not forget sequence removals that can reappear.
        std::fs::File::open(store::epoch(&self.root, epoch))?.sync_all()?;
        let mut cache = self.lock()?;
        let index = cache
            .index
            .as_mut()
            .ok_or_else(|| invalid("mutation log cleanup owner"))?;
        if index.active.as_ref() == Some(epoch) {
            return Err(invalid("active mutation log cannot be removed"));
        }
        let mut saved = index.clone();
        saved.epochs.retain(|id| id != epoch);
        if !saved.cleanup.contains(epoch) {
            saved.cleanup.push(epoch.clone());
        }
        store::write(&self.root.join("index.json"), &saved)?;
        *index = saved;
        Ok(())
    }

    fn remove_entry(&self, epoch: &Id, sequence: u64, incomplete: bool) -> StorageResult<()> {
        let directory = store::entry(&self.root, epoch, sequence)?;
        if !directory.try_exists()? {
            return Ok(());
        }
        store::directory(&directory)?;
        let entry: Option<LogEntry> = store::read(&directory.join(if incomplete {
            "pending.json"
        } else {
            "entry.json"
        }))?;
        if let Some(entry) = entry {
            if entry.epoch != *epoch || entry.sequence != sequence || entry.parts.len() > 3 {
                return Err(invalid("mutation log cleanup entry identity"));
            }
            parts(&directory, &entry)?;
        }
        for role in [CommitRole::CasAdd, CommitRole::Main, CommitRole::CasDelete] {
            store::remove_empty(&directory.join(role.directory()))?;
        }
        for name in [
            "entry.json",
            "entry.pending",
            "pending.json",
            "pending.pending",
        ] {
            store::remove_metadata(&directory.join(name))?;
        }
        store::remove_empty(&directory)
    }

    fn finish_cleanup(&self) -> StorageResult<()> {
        let retired = self
            .lock()?
            .index
            .as_ref()
            .map_or_else(Vec::new, |index| index.cleanup.clone());
        for epoch in retired {
            let directory = store::epoch(&self.root, &epoch);
            if directory.try_exists()? {
                for name in ["state.json", "state.pending"] {
                    store::remove_metadata(&directory.join(name))?;
                }
                store::remove_empty(&directory)?;
            }
            let mut cache = self.lock()?;
            let index = cache
                .index
                .as_mut()
                .ok_or_else(|| invalid("mutation log retired cleanup owner"))?;
            let mut saved = index.clone();
            saved.cleanup.retain(|id| id != &epoch);
            store::write(&self.root.join("index.json"), &saved)?;
            *index = saved;
        }
        Ok(())
    }
}
