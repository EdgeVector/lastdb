//! A native write keeps each new run until its complete journal marker is durable.
use super::*;
use laststore::disk_run::home::RunCommit;

pub(in crate::storage::disk_pack) struct Recording<'a> {
    log: &'a MutationLog,
    _owner: MutexGuard<'a, Cache>,
    state: EpochState,
    entry: LogEntry,
    directory: PathBuf,
    earlier_bytes: u64,
    failed: bool,
}

impl MutationLog {
    pub fn begin(&self, v2: &V2Home) -> Option<Recording<'_>> {
        match Recording::new(self, v2) {
            Ok(recording) => recording,
            Err(error) => {
                tracing::warn!(target: "fold_db::mutation_log", error = %error,
                    "native mutation log needs a snapshot; local writes keep their native durability");
                None
            }
        }
    }
}

impl<'a> Recording<'a> {
    fn new(log: &'a MutationLog, v2: &V2Home) -> StorageResult<Option<Self>> {
        let owner = log.lock()?;
        let Some(index) = &owner.index else {
            return Ok(None);
        };
        if !index.enabled {
            return Ok(None);
        }
        let Some(epoch) = &index.active else {
            return Ok(None);
        };
        let mut state = log.state(epoch)?;
        if state.gap.is_some() {
            return Ok(None);
        }
        if state.head.frontiers != frontiers(v2) {
            state.gap = Some("native frontier exceeds the complete mutation log".to_owned());
            store::write(&store::epoch(&log.root, epoch).join("state.json"), &state)?;
            return Ok(None);
        }
        let sequence = state
            .head
            .sequence
            .checked_add(1)
            .filter(|n| *n <= store::MAX_ENTRIES)
            .ok_or_else(|| invalid("mutation log entry resource bound"))?;
        let directory = store::entry(&log.root, epoch, sequence)?;
        store::create(&directory)?;
        let entry = LogEntry {
            version: 1,
            epoch: epoch.clone(),
            sequence,
            previous_sha256: state.head.sha256.clone(),
            before: state.head.frontiers,
            after: state.head.frontiers,
            parts: Vec::new(),
        };
        store::write(&directory.join("pending.json"), &entry)?;
        let earlier_bytes = log.charge(index)?;
        Ok(Some(Self {
            log,
            _owner: owner,
            state,
            entry,
            directory,
            earlier_bytes,
            failed: false,
        }))
    }

    pub fn retain(&mut self, role: CommitRole, commit: &RunCommit) {
        if self.failed {
            return;
        }
        if let Err(error) = self.retain_inner(role, commit) {
            self.failed = true;
            self.state.gap = Some(
                "native mutation log byte retention failed; a snapshot is required".to_owned(),
            );
            tracing::warn!(target: "fold_db::mutation_log", error = %error,
                "native mutation log retained its incomplete entry for recovery");
        }
    }

    fn retain_inner(&mut self, role: CommitRole, commit: &RunCommit) -> StorageResult<()> {
        if self
            .entry
            .parts
            .last()
            .is_some_and(|part| part.role >= role)
        {
            return Err(invalid("mutation log native commit order"));
        }
        let frontier = if role.is_cas() {
            &mut self.entry.after.cas
        } else {
            &mut self.entry.after.main
        };
        if frontier.checked_add(1) != Some(commit.frontier) {
            return Err(invalid("mutation log native commit frontier"));
        }
        *frontier = commit.frontier;
        let part = LogPart {
            role,
            run_id: Id::try_from(commit.run_id.to_string())
                .map_err(|_| invalid("mutation log native file UUID"))?,
            bytes: commit.run_bytes,
            frontier: commit.frontier,
        };
        let quote = self
            .earlier_bytes
            .checked_add(self.entry.bytes()?)
            .and_then(|n| n.checked_add(part.bytes))
            .and_then(|n| n.checked_add(4 * store::MAX_JSON))
            .ok_or_else(|| invalid("mutation log backlog size overflow"))?;
        if quote > lastdb_storage_policy::BACKUP_RESOURCES.durable_spool_bytes {
            return Err(invalid(
                "mutation log backlog resource bound; snapshot required",
            ));
        }
        let parent = self.directory.join(role.directory());
        store::create(&parent)?;
        self.entry.parts.push(part);
        // Intent precedes the hardlink. Recovery counts even an uncertain local allocation.
        store::write(&self.directory.join("pending.json"), &self.entry)?;
        commit
            .retain_file(&parent.join(format!("{}.ldrun", commit.run_id)))
            .map_err(super::super::map)
    }

    pub fn finish(mut self, v2: &V2Home) {
        let result = self.finish_inner(v2);
        if let Err(error) = result {
            self.state.gap =
                Some("native mutation log commit marker failed; a snapshot is required".to_owned());
            let _ = store::write(
                &store::epoch(&self.log.root, &self.entry.epoch).join("state.json"),
                &self.state,
            );
            tracing::warn!(target: "fold_db::mutation_log", error = %error,
                "native mutation log needs recovery; no cloud frontier advanced");
        }
    }

    fn finish_inner(&mut self, v2: &V2Home) -> StorageResult<()> {
        if self.failed {
            return store::write(
                &store::epoch(&self.log.root, &self.entry.epoch).join("state.json"),
                &self.state,
            );
        }
        if self.entry.after != frontiers(v2) {
            return Err(invalid("mutation log complete native batch"));
        }
        let position = self.entry.position_after(&self.state.head)?;
        store::write(&self.directory.join("entry.json"), &self.entry)?;
        let bytes = self
            .state
            .retained_bytes
            .checked_add(self.entry.bytes()?)
            .ok_or_else(|| invalid("mutation log retained size overflow"))?;
        let mut next = self.state.clone();
        next.head = position;
        next.retained_bytes = bytes;
        let metadata = cloud_backup_protocol::v2::hash::canonical_bytes(&self.entry)
            .map_err(|_| invalid("mutation log canonical entry"))?
            .len() as u64;
        next.metadata_bytes = next
            .metadata_bytes
            .checked_add(2 * metadata)
            .ok_or_else(|| invalid("mutation log metadata size overflow"))?;
        store::write(
            &store::epoch(&self.log.root, &self.entry.epoch).join("state.json"),
            &next,
        )?;
        self.state = next;
        Ok(())
    }
}
