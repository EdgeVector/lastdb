//! Stage one fixed accepted mutation-log root: a private snapshot followed by native batches.
use super::{
    ensure_directory, prepare_native_ephemera,
    runs::{self, load_closure, snapshot_closure, stage_pages},
    sync_dir, ByteBackupReader, GuestReplayCache, GuestSecrets, RestoreError as E, RestoreSession,
    Result,
};
use cloud_backup_protocol::v2::{AcceptedBackup, Begin, Destination};
use cloud_backup_protocol::{Digest, RecoveryDescriptor};
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::{
    byte_backup::{
        decode_metadata, valid_page_refs, validate_encoded_page, validate_metadata_context,
        ClientPageReference, MAX_CLIENT_PAGES,
    },
    mutation_log::{log_path, LogAnchor, LogEntry, LogPosition},
    run_restore,
};
use serde::Deserialize;
use std::{collections::BTreeSet, fs, path::Path};

const LOG_VERSION: u16 = 5;
const MAX_BATCH_ENTRIES: usize = 128;
const MAX_CHAIN: usize = 32768;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum LogKind {
    NativeMutationLog,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationLogPage {
    version: u16,
    page_index: u32,
    begin_sha256: Digest,
    descriptor_sha256: Digest,
    files: Vec<ClientNativeFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
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
        let root: Self =
            decode_metadata(bytes, key).map_err(|_| E::Invalid("mutation log metadata"))?;
        root.validate_for(begin).map(|()| root)
    }

    pub fn validate_for(&self, begin: &Begin) -> Result<()> {
        let ctx = (&self.begin_sha256, &self.descriptor_sha256);
        check_log_context(self.version, ctx, begin)?;
        self.base
            .validate()
            .and_then(|()| self.previous.validate())
            .and_then(|()| self.recovery.validate_for(&begin.job.stream.scope))?;
        let anchor = self
            .anchor
            .position()
            .map_err(|_| E::Invalid("mutation log anchor"))?;
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
            return Err(E::Invalid("mutation log root selection or boundary"));
        }
        let (mut pos, mut files, mut bytes) = (self.from.clone(), 0u64, 0u64);
        for e in &self.entries {
            pos = e
                .position_after(&pos)
                .map_err(|_| E::Invalid("mutation log sequence"))?;
            files = files
                .checked_add(e.parts.len() as u64)
                .ok_or(E::Limit("mutation log file count"))?;
            let sz = e
                .bytes()
                .map_err(|_| E::Invalid("mutation log entry size"))?;
            bytes = bytes
                .checked_add(sz)
                .ok_or(E::Limit("mutation log byte count"))?;
        }
        (pos == self.through
            && (files, bytes) == (self.native_file_count, self.native_bytes)
            && valid_page_refs(&self.pages))
        .then_some(())
        .ok_or(E::Invalid("mutation log complete range totals"))
    }

    fn decode_page(
        &self,
        bytes: &[u8],
        key: &[u8; 32],
        begin: &Begin,
        index: u32,
    ) -> Result<Vec<ClientNativeFile>> {
        let p: MutationLogPage =
            decode_metadata(bytes, key).map_err(|_| E::Invalid("mutation file page"))?;
        validate_encoded_page(
            (p.version, LOG_VERSION),
            p.page_index,
            (&p.begin_sha256, &p.descriptor_sha256),
            &p.files,
            begin,
        )
        .map_err(|_| E::Invalid("mutation file page"))?;
        check_log_context(p.version, (&p.begin_sha256, &p.descriptor_sha256), begin)?;
        if p.version != self.version
            || p.page_index != index
            || p.begin_sha256 != self.begin_sha256
            || p.descriptor_sha256 != self.descriptor_sha256
            || (index as usize) >= self.pages.len()
        {
            return Err(E::Invalid("mutation file page context"));
        }
        for file in &p.files {
            if file.path.as_str().split('/').nth(3) != Some("main") {
                return Err(E::Invalid("mutation route role"));
            }
            let found = self
                .entries
                .iter()
                .flat_map(|e| e.parts.iter().map(move |pt| (e.sequence, pt)))
                .find(|(seq, pt)| {
                    file.path.as_str() == log_path(&self.anchor.epoch, *seq, pt.role, &pt.run_id)
                });
            if !matches!(found, Some((_, pt)) if file.native_bytes.get() == pt.bytes) {
                return Err(E::Invalid("mutation native file identity"));
            }
        }
        Ok(p.files)
    }
}

fn check_log_context(
    version: u16,
    (digest, desc): (&Digest, &Digest),
    begin: &Begin,
) -> Result<()> {
    validate_metadata_context(version, LOG_VERSION, digest, desc, begin)
        .map_err(|_| E::Invalid("mutation log metadata frozen context"))?;
    (begin.required_destinations == [Destination::R2, Destination::B2])
        .then_some(())
        .ok_or(E::Invalid("mutation log metadata frozen context"))
}

pub(super) async fn stage_guest(
    session: &RestoreSession,
    client: &ByteBackupReader,
    secrets: &GuestSecrets,
    latest: MutationLogRoot,
    cache: Option<&GuestReplayCache<'_>>,
) -> Result<(run_restore::RunNodeGuard, LogPosition)> {
    if latest.recovery != secrets.recovery {
        return Err(E::Invalid("recovery public key"));
    }
    let (chain, cached) = prepare(session, client, secrets, &latest, cache).await?;
    let mut start = if let Some((accepted, pos)) = cached {
        let c = cache.ok_or(E::Invalid("cached mutation predecessor"))?;
        let data = session.root.join("data");
        run_restore::RunSnapshotReader::open(c.data, &secrets.key, c.native_root)
            .and_then(|r| r.seed_replay(&data, &latest.anchor, &pos))
            .map_err(E::native_proof)?;
        prepare_native_ephemera(&data, &secrets.key)?;
        (accepted, pos)
    } else {
        let base = session.ancestor(latest.base.clone())?;
        let (mut inv, bytes) = load_closure(&base, client).await?;
        let root = snapshot_closure(&base, secrets, &bytes)?;
        if root.recovery != latest.recovery
            || root.run.main.home_id != latest.anchor.main_home
            || root.run.main.frontier.get() != latest.anchor.frontiers.main
            || !base.accepted.begin.dependencies().is_empty()
        {
            return Err(E::Invalid("mutation exact snapshot boundary"));
        }
        runs::stage_and_verify(&base, client, secrets, &root, &mut inv).await?;
        (
            latest.base.clone(),
            latest.anchor.position().map_err(E::native_proof)?,
        )
    };
    replay(session, client, secrets, &latest, chain, &mut start)
        .await
        .map(|g| (g, latest.through))
}

async fn prepare(
    session: &RestoreSession,
    client: &ByteBackupReader,
    secrets: &GuestSecrets,
    latest: &MutationLogRoot,
    cache: Option<&GuestReplayCache<'_>>,
) -> Result<(Vec<AcceptedBackup>, Option<(AcceptedBackup, LogPosition)>)> {
    let (mut accepted, mut root, mut chain) =
        (session.accepted.clone(), latest.clone(), Vec::new());
    let cached = loop {
        if chain.len() >= MAX_CHAIN {
            return Err(E::Limit("mutation restore chain"));
        }
        root.validate_for(&accepted.begin)
            .map_err(|_| E::Invalid("mutation checkpoint context"))?;
        if (&root.base, &root.anchor, &root.recovery)
            != (&latest.base, &latest.anchor, &latest.recovery)
            || root.previous.ordinal.get() >= accepted.ordinal.get()
        {
            return Err(E::Invalid("mutation predecessor chain"));
        }
        chain.push(accepted.clone());
        if let Some(c) = cache.filter(|c| root.previous.head() == *c.head) {
            if !c
                .through
                .map_or(root.previous == root.base, |s| *s == root.from)
            {
                return Err(E::Invalid("cached mutation checkpoint predecessor"));
            }
            break Some((root.previous.clone(), root.from.clone()));
        }
        if root.previous == latest.base {
            break None;
        }
        let ancestor = session.ancestor(root.previous.clone())?;
        let (_, bytes) = load_closure(&ancestor, client).await?;
        let prev = MutationLogRoot::decode(&bytes, &secrets.key, &ancestor.accepted.begin)
            .map_err(|_| E::Invalid("mutation ancestor root"))?;
        if root.base != prev.base || root.anchor != prev.anchor || root.from != prev.through {
            return Err(E::Invalid("mutation ancestor prefix"));
        }
        accepted = root.previous;
        root = prev;
    };
    Ok((chain, cached))
}

async fn replay(
    session: &RestoreSession,
    client: &ByteBackupReader,
    secrets: &GuestSecrets,
    latest: &MutationLogRoot,
    chain: Vec<AcceptedBackup>,
    cursor: &mut (AcceptedBackup, LogPosition),
) -> Result<run_restore::RunNodeGuard> {
    ensure_directory(&session.root.join("replay-files"))?;
    sync_dir(&session.root)?;
    for accepted in chain.into_iter().rev() {
        replay_checkpoint(session, client, secrets, latest, accepted, cursor).await?;
    }
    if cursor.0 != session.accepted || cursor.1 != latest.through {
        return Err(E::Invalid("mutation complete selected prefix"));
    }
    let data = session.root.join("data");
    run_restore::MutationReplay::open(&data, &secrets.key, &latest.anchor)
        .map_err(|_| E::Invalid("final native replay admission"))?
        .settle()
        .map_err(|_| E::Invalid("final native replay compaction"))?;
    run_restore::verify_replayed(&data, &latest.anchor, &latest.through, &secrets.key)
        .map_err(E::native_proof)
}

async fn replay_checkpoint(
    session: &RestoreSession,
    client: &ByteBackupReader,
    secrets: &GuestSecrets,
    latest: &MutationLogRoot,
    accepted: AcceptedBackup,
    cursor: &mut (AcceptedBackup, LogPosition),
) -> Result<()> {
    let ancestor = session.ancestor(accepted.clone())?;
    let (mut inv, bytes) = load_closure(&ancestor, client).await?;
    let root = MutationLogRoot::decode(&bytes, &secrets.key, &accepted.begin)
        .map_err(|_| E::Invalid("replayed checkpoint root"))?;
    if (&root.base, &root.anchor, &root.recovery)
        != (&latest.base, &latest.anchor, &latest.recovery)
        || root.previous != cursor.0
        || root.from != cursor.1
    {
        return Err(E::Invalid("replayed checkpoint predecessor"));
    }
    let objs =
        u64::try_from(inv.receipts.len()).map_err(|_| E::Limit("mutation route object count"))?;
    let meta_objs = (root.pages.len() as u64)
        .checked_add(1)
        .ok_or(E::Limit("mutation route metadata count"))?;
    let replay_dir = session.root.join("replay-files");
    stage_pages(
        &ancestor,
        client,
        &root.pages,
        (root.native_file_count, root.native_bytes),
        &mut inv,
        &replay_dir,
        |i, b| root.decode_page(b, &secrets.key, &accepted.begin, i),
    )
    .await?;
    if objs != accepted.inventory_root.object_count.get() || meta_objs > objs {
        return Err(E::Invalid("complete mutation route totals"));
    }
    let replay =
        run_restore::MutationReplay::open(&session.root.join("data"), &secrets.key, &root.anchor)
            .map_err(|_| E::Invalid("private native replay admission"))?;
    for entry in &root.entries {
        for (idx, part) in entry.parts.iter().enumerate() {
            let rel = log_path(&root.anchor.epoch, entry.sequence, part.role, &part.run_id);
            replay
                .part(entry, idx, &replay_dir.join(rel))
                .map_err(|_| E::Invalid("original native mutation part replay"))?;
        }
        cursor.1 = entry
            .position_after(&cursor.1)
            .map_err(|_| E::Invalid("complete replay batch"))?;
    }
    if replay.frontiers() != root.through.frontiers || cursor.1 != root.through {
        return Err(E::Invalid("complete checkpoint frontier"));
    }
    release_checkpoint(&replay_dir, &root)?;
    cursor.0 = accepted;
    replay
        .settle()
        .map_err(|_| E::Invalid("checkpoint compaction"))
}

fn release_checkpoint(base: &Path, root: &MutationLogRoot) -> Result<()> {
    let mut dirs = BTreeSet::from([base.to_path_buf()]);
    for entry in &root.entries {
        for part in &entry.parts {
            let path = base.join(log_path(
                &root.anchor.epoch,
                entry.sequence,
                part.role,
                &part.run_id,
            ));
            match fs::symlink_metadata(&path) {
                Ok(m) if m.is_file() && m.len() == part.bytes => fs::remove_file(&path)?,
                Ok(_) => return Err(E::Invalid("mutation source release identity")),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            dirs.insert(
                path.parent()
                    .ok_or(E::Invalid("source parent"))?
                    .to_path_buf(),
            );
        }
    }
    for d in &dirs {
        sync_dir(d)?;
    }
    Ok(())
}
