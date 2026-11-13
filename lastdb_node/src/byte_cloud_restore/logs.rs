//! Stage one fixed accepted mutation-log root: a private snapshot followed by native batches.
use super::{
    local,
    runs::{self, load_closure, snapshot_closure, Inventory},
    ByteBackupReader, GuestReplayCache, RecoveryDecryption, RestoreError, RestoreSession, Result,
};
use cloud_backup_protocol::v2::{AcceptedBackup, Begin, Destination};
use cloud_backup_protocol::{Digest, RecoveryDescriptor};
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::{
    byte_backup::{
        check_metadata_size, decode_metadata, valid_page_refs, validate_metadata_context,
        ClientPageReference, ClientRoot, MAX_CLIENT_PAGES,
    },
    mutation_log::{log_path, LogAnchor, LogEntry, LogPosition},
    run_restore,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

pub(super) const LOG_VERSION: u16 = 5;
pub(super) const MAX_BATCH_ENTRIES: usize = 128;
const MAX_CHAIN: u32 = 32768;
const CHAIN_RESERVE: u64 = 2 * 256 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum LogKind {
    NativeMutationLog,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MutationLogRoot {
    pub version: u16,
    pub kind: LogKind,
    pub begin_sha256: Digest,
    pub descriptor_sha256: Digest,
    pub recovery: RecoveryDescriptor,
    pub base: AcceptedBackup,
    pub previous: AcceptedBackup,
    pub anchor: LogAnchor,
    pub from: LogPosition,
    pub through: LogPosition,
    pub entries: Vec<LogEntry>,
    pub native_file_count: u64,
    pub native_bytes: u64,
    pub pages: Vec<ClientPageReference>,
}

impl MutationLogRoot {
    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> Result<Self> {
        let root: Self = decode_metadata(bytes, key)
            .map_err(|_| RestoreError::Invalid("mutation log metadata"))?;
        root.validate_for(begin).map(|()| root)
    }

    pub fn validate_for(&self, begin: &Begin) -> Result<()> {
        let (b_sha, d_sha) = (&self.begin_sha256, &self.descriptor_sha256);
        validate_metadata_context(self.version, LOG_VERSION, b_sha, d_sha, begin)
            .map_err(|_| RestoreError::Invalid("mutation log metadata frozen context"))?;
        require_log_destinations(begin)?;
        self.base
            .validate()
            .and_then(|()| self.previous.validate())
            .and_then(|()| self.recovery.validate_for(&begin.job.stream.scope))?;
        let anchor = self
            .anchor
            .position()
            .map_err(|_| RestoreError::Invalid("mutation log anchor"))?;
        let deps = if self.previous == self.base {
            vec![self.base.head()]
        } else {
            vec![self.base.head(), self.previous.head()]
        };
        if begin.recovery_dependencies != deps
            || self.base.job().stream != begin.job.stream
            || self.previous.job().stream != begin.job.stream
            || begin.predecessor.as_ref() != Some(&self.previous.head())
            || self.base.ordinal.get() > self.previous.ordinal.get()
            || !(1..=MAX_BATCH_ENTRIES).contains(&self.entries.len())
            || self.from.epoch != self.anchor.epoch
            || self.through.epoch != self.anchor.epoch
            || !(1..=MAX_CLIENT_PAGES).contains(&self.pages.len())
            || ((self.from.sequence == 0 || self.previous == self.base) && self.from != anchor)
        {
            return Err(RestoreError::Invalid(
                "mutation log root selection or boundary",
            ));
        }
        let (mut pos, mut files, mut bytes) = (self.from.clone(), 0u64, 0u64);
        for e in &self.entries {
            pos = e
                .position_after(&pos)
                .map_err(|_| RestoreError::Invalid("mutation log sequence"))?;
            files = files
                .checked_add(e.parts.len() as u64)
                .ok_or(RestoreError::Limit("mutation log file count"))?;
            let sz = e
                .bytes()
                .map_err(|_| RestoreError::Invalid("mutation log entry size"))?;
            bytes = bytes
                .checked_add(sz)
                .ok_or(RestoreError::Limit("mutation log byte count"))?;
        }
        if (pos, files, bytes)
            != (
                self.through.clone(),
                self.native_file_count,
                self.native_bytes,
            )
            || !valid_page_refs(&self.pages)
        {
            return Err(RestoreError::Invalid("mutation log complete range totals"));
        }
        check_metadata_size(self).map_err(|_| RestoreError::Invalid("mutation log metadata size"))
    }

    pub fn validate_page(&self, page: &local::MutationLogPage) -> Result<()> {
        if (page.version, &page.begin_sha256, &page.descriptor_sha256)
            != (self.version, &self.begin_sha256, &self.descriptor_sha256)
            || self.pages.get(page.page_index as usize).is_none()
        {
            return Err(RestoreError::Invalid("mutation file page context"));
        }
        page.files
            .iter()
            .try_for_each(|f| self.validate_file(f).map(|_| ()))
    }

    pub fn validate_file(&self, file: &ClientNativeFile) -> Result<Destination> {
        file.validate()
            .map_err(|_| RestoreError::Invalid("mutation file extents"))?;
        let epoch = &self.anchor.epoch;
        let found = self
            .entries
            .iter()
            .flat_map(|e| e.parts.iter().map(move |p| (e.sequence, p)))
            .find(|(seq, p)| file.path.as_str() == log_path(epoch, *seq, p.role, &p.run_id));
        match found {
            Some((_, p)) if file.native_bytes.get() == p.bytes => Ok(Destination::R2),
            _ => Err(RestoreError::Invalid("mutation native file identity")),
        }
    }

    fn validate_previous(&self, prev: &Self) -> Result<()> {
        (self.base == prev.base && self.anchor == prev.anchor && self.from == prev.through)
            .then_some(())
            .ok_or(RestoreError::Invalid("mutation ancestor prefix"))
    }
}

pub(super) fn require_log_destinations(begin: &Begin) -> Result<()> {
    (begin.required_destinations == [Destination::R2, Destination::B2])
        .then_some(())
        .ok_or(RestoreError::Invalid(
            "mutation log metadata frozen context",
        ))
}

pub(super) fn link_path(session: &RestoreSession, index: u32) -> PathBuf {
    session.root.join(format!("chain/link-{index:08}.json"))
}

pub(super) async fn stage_guest(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: MutationLogRoot,
    cache: Option<&GuestReplayCache<'_>>,
) -> Result<(run_restore::RunRestoreGuard, LogPosition)> {
    decryption.validate_account(&latest.recovery.account_public_key)?;
    let (links, cached) = prepare(session, client, decryption, &latest, cache).await?;
    let (previous, position) = if let Some((accepted, pos)) = cached {
        let c = cache.ok_or(RestoreError::Invalid("cached mutation predecessor"))?;
        let (data, key) = (session.root.join("home/data"), &decryption.content);
        run_restore::RunSnapshotReader::open(c.data, key, c.native_root)
            .and_then(|r| r.seed_replay(&data, &latest.anchor, &pos))
            .map_err(RestoreError::native_proof)?;
        local::prepare_native_ephemera(&data, key)?;
        (accepted, pos)
    } else {
        let (base, root, inv) = load_base(session, client, decryption, &latest).await?;
        runs::stage_and_verify(&base, client, decryption, &root, inv).await?;
        let pos = latest
            .anchor
            .position()
            .map_err(RestoreError::native_proof)?;
        (latest.base.clone(), pos)
    };
    let guard = replay(
        session, client, decryption, &latest, links, previous, position,
    )
    .await?;
    Ok((guard, latest.through))
}

async fn prepare(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    cache: Option<&GuestReplayCache<'_>>,
) -> Result<(u32, Option<(AcceptedBackup, LogPosition)>)> {
    let (mut accepted, mut root, mut count) = (session.accepted().clone(), latest.clone(), 0);
    let cached = loop {
        if count >= MAX_CHAIN {
            return Err(RestoreError::Limit("mutation restore chain"));
        }
        root.validate_for(&accepted.begin)
            .map_err(|_| RestoreError::Invalid("mutation checkpoint context"))?;
        if (&root.base, &root.anchor, &root.recovery)
            != (&latest.base, &latest.anchor, &latest.recovery)
            || root.previous.ordinal.get() >= accepted.ordinal.get()
        {
            return Err(RestoreError::Invalid("mutation predecessor chain"));
        }
        let lpath = link_path(session, count);
        session.save_or_verify_spool_json(&lpath, CHAIN_RESERVE, &accepted, "saved ancestor")?;
        count += 1;
        session.clear_cache()?;
        if cache.is_some_and(|c| root.previous.head() == *c.head) {
            let ok = cache.is_some_and(|c| {
                root.previous.head() == *c.head
                    && c.through
                        .map_or(root.previous == root.base, |s| *s == root.from)
            });
            if !ok {
                return Err(RestoreError::Invalid(
                    "cached mutation checkpoint predecessor",
                ));
            }
            break Some((root.previous.clone(), root.from.clone()));
        }
        if root.previous == latest.base {
            break None;
        }
        let ancestor = session.ancestor(root.previous.clone())?;
        let (_, bytes) = load_closure(&ancestor, client).await?;
        let prev = MutationLogRoot::decode(&bytes, &decryption.content, &ancestor.accepted().begin)
            .map_err(|_| RestoreError::Invalid("mutation ancestor root"))?;
        root.validate_previous(&prev)?;
        accepted = root.previous;
        root = prev;
    };
    Ok((count, cached))
}

async fn load_base(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
) -> Result<(RestoreSession, ClientRoot, Inventory)> {
    let base = session.ancestor(latest.base.clone())?;
    let (inventory, bytes) = load_closure(&base, client).await?;
    let root = snapshot_closure(&base, decryption, &bytes)?;
    if root.recovery != latest.recovery
        || root.run.main.home_id != latest.anchor.main_home
        || root.run.main.frontier.get() != latest.anchor.frontiers.main
        || !base.accepted().begin.dependencies().is_empty()
    {
        return Err(RestoreError::Invalid("mutation exact snapshot boundary"));
    }
    Ok((base, root, inventory))
}

async fn replay(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    mut remaining: u32,
    mut previous: AcceptedBackup,
    mut position: LogPosition,
) -> Result<run_restore::RunRestoreGuard> {
    local::ensure_directory(&session.root.join("replay-files"))?;
    local::sync_dir(&session.root)?;
    while remaining > 0 {
        let idx = remaining - 1;
        replay_checkpoint(
            session,
            client,
            decryption,
            latest,
            idx,
            &mut previous,
            &mut position,
        )
        .await?;
        remaining = idx;
    }
    if previous != *session.accepted() || position != latest.through {
        return Err(RestoreError::Invalid("mutation complete selected prefix"));
    }
    let (data, key) = (session.root.join("home/data"), &decryption.content);
    run_restore::MutationReplay::open(&data, key, &latest.anchor)
        .map_err(|_| RestoreError::Invalid("final native replay admission"))?
        .settle()
        .map_err(|_| RestoreError::Invalid("final native replay compaction"))?;
    run_restore::verify_replayed(&data, &latest.anchor, &latest.through, key)
        .map_err(RestoreError::native_proof)
}

async fn replay_checkpoint(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    idx: u32,
    previous: &mut AcceptedBackup,
    position: &mut LogPosition,
) -> Result<()> {
    let accepted: AcceptedBackup = local::read_json(&link_path(session, idx), 256 * 1024)?;
    let ancestor = session.ancestor(accepted.clone())?;
    let (inventory, bytes) = load_closure(&ancestor, client).await?;
    let key = &decryption.content;
    let root = MutationLogRoot::decode(&bytes, key, &accepted.begin)
        .map_err(|_| RestoreError::Invalid("replayed checkpoint root"))?;
    if (&root.base, &root.anchor, &root.recovery)
        != (&latest.base, &latest.anchor, &latest.recovery)
        || root.previous != *previous
        || *position != root.from
    {
        return Err(RestoreError::Invalid("replayed checkpoint predecessor"));
    }
    local::download_checkpoint(&ancestor, client, decryption, &root, inventory, idx).await?;
    let data = session.root.join("home/data");
    let replay = run_restore::MutationReplay::open(&data, key, &root.anchor)
        .map_err(|_| RestoreError::Invalid("private native replay admission"))?;
    for entry in &root.entries {
        for (part_idx, part) in entry.parts.iter().enumerate() {
            let rel = log_path(&root.anchor.epoch, entry.sequence, part.role, &part.run_id);
            let pfile = session.root.join("replay-files").join(rel);
            replay
                .part(entry, part_idx, &pfile)
                .map_err(|_| RestoreError::Invalid("original native mutation part replay"))?;
        }
        *position = entry
            .position_after(position)
            .map_err(|_| RestoreError::Invalid("complete replay batch"))?;
    }
    if replay.frontiers() != root.through.frontiers || *position != root.through {
        return Err(RestoreError::Invalid("complete checkpoint frontier"));
    }
    release_checkpoint(session, &root)?;
    *previous = accepted;
    replay
        .settle()
        .map_err(|_| RestoreError::Invalid("checkpoint compaction"))
}

fn release_checkpoint(session: &RestoreSession, root: &MutationLogRoot) -> Result<()> {
    let mut dirs = BTreeSet::from([
        session.root.join("entries"),
        session.root.join("replay-files"),
    ]);
    for entry in &root.entries {
        for part in &entry.parts {
            let name = log_path(&root.anchor.epoch, entry.sequence, part.role, &part.run_id);
            let path = session.root.join("replay-files").join(&name);
            match fs::symlink_metadata(&path) {
                Ok(m) if m.is_file() && m.len() == part.bytes => fs::remove_file(&path)?,
                Ok(_) => return Err(RestoreError::Invalid("mutation source release identity")),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            let parent = path
                .parent()
                .ok_or(RestoreError::Invalid("source parent"))?;
            dirs.insert(parent.to_path_buf());
            session
                .remove_spool_file(&session.root.join("entries").join(local::path_key(&name)))?;
        }
    }
    dirs.iter().try_for_each(|d| local::sync_dir(d))?;
    session.clear_cache()
}
