//! Stage one fixed accepted mutation-log root: a private snapshot followed by native batches.
use super::{
    local,
    runs::{self, load_closure, snapshot_closure, Inventory},
    ByteBackupReader, GuestReplayCache, RecoveryDecryption, RestoreError, RestoreSession, Result,
};
use crate::byte_cloud_log::MutationLogRoot;
use cloud_backup_protocol::v2::AcceptedBackup;
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::{
    byte_backup::ClientRoot,
    mutation_log::{log_path, LogPosition},
    run_restore,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

pub(super) const MAX_CHAIN: u32 = 32768;
pub(super) const CHAIN_RESERVE: u64 = 2 * 256 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    version: u16,
    head: Digest,
    base: AcceptedBackup,
    links: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cached: Option<CachedStart>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CachedStart {
    accepted: AcceptedBackup,
    position: LogPosition,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u16,
    head: Digest,
    remaining: u32,
    previous: AcceptedBackup,
    position: LogPosition,
    entry: usize,
    part: usize,
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
    let plan = prepare(session, client, decryption, &latest, cache).await?;
    let cursor = if let Some(c) = load_cursor(session, &plan)? {
        c
    } else {
        if let Some(start) = &plan.cached {
            let c = cache.ok_or(RestoreError::Invalid("cached mutation predecessor"))?;
            let (data, key) = (session.root.join("home/data"), &decryption.content);
            run_restore::RunSnapshotReader::open(c.data, key, c.native_root)
                .and_then(|r| r.seed_replay(&data, &latest.anchor, &start.position))
                .map_err(RestoreError::native_proof)?;
            local::prepare_native_ephemera(&data, key)?;
        } else {
            let (base, root, inv) = load_base(session, client, decryption, &latest).await?;
            runs::stage_and_verify(&base, client, decryption, &root, inv).await?;
        }
        start_cursor(session, &plan, &latest)?
    };
    Ok((
        replay(session, client, decryption, &latest, cursor).await?,
        latest.through,
    ))
}

async fn prepare(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    cache: Option<&GuestReplayCache<'_>>,
) -> Result<Plan> {
    let path = session.root.join("chain/plan.json");
    if path.try_exists()? {
        let p: Plan = local::read_json(&path, 256 * 1024)?;
        let bad_cache = p
            .cached
            .as_ref()
            .is_some_and(|s| !matches_cache(cache, &s.accepted, &s.position, &latest.base));
        if p.version != 1
            || p.head != session.accepted().commit_sha256
            || p.base != latest.base
            || !(1..=MAX_CHAIN).contains(&p.links)
            || bad_cache
        {
            return Err(RestoreError::Invalid("saved mutation restore plan"));
        }
        return Ok(p);
    }
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
            if !matches_cache(cache, &root.previous, &root.from, &root.base) {
                return Err(RestoreError::Invalid(
                    "cached mutation checkpoint predecessor",
                ));
            }
            break Some(CachedStart {
                accepted: root.previous.clone(),
                position: root.from.clone(),
            });
        }
        if root.previous == latest.base {
            break None;
        }
        let ancestor = session.ancestor(root.previous.clone())?;
        let (_, bytes) = load_closure(&ancestor, client).await?;
        let prev = MutationLogRoot::decode(&bytes, &decryption.content, &ancestor.accepted().begin)
            .map_err(|_| RestoreError::Invalid("mutation ancestor root"))?;
        root.validate_previous(&prev)
            .map_err(|_| RestoreError::Invalid("mutation ancestor prefix"))?;
        accepted = root.previous;
        root = prev;
    };
    let plan = Plan {
        version: 1,
        head: session.accepted().commit_sha256.clone(),
        base: latest.base.clone(),
        links: count,
        cached,
    };
    session
        .write_spool_json(&path, CHAIN_RESERVE, &plan)
        .map(|()| plan)
}

fn matches_cache(
    cache: Option<&GuestReplayCache<'_>>,
    accepted: &AcceptedBackup,
    position: &LogPosition,
    base: &AcceptedBackup,
) -> bool {
    cache.is_some_and(|c| {
        accepted.head() == *c.head && c.through.map_or(accepted == base, |s| *s == *position)
    })
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

fn save_cursor(session: &RestoreSession, cursor: &Cursor) -> Result<()> {
    session.write_spool_json(&session.root.join("chain/cursor.json"), 512 * 1024, cursor)
}

fn load_cursor(session: &RestoreSession, plan: &Plan) -> Result<Option<Cursor>> {
    let path = session.root.join("chain/cursor.json");
    if !path.try_exists()? {
        return Ok(None);
    }
    let c: Cursor = local::read_json(&path, 256 * 1024)?;
    (c.version == 2 && c.head == plan.head && c.remaining <= plan.links)
        .then_some(Some(c))
        .ok_or(RestoreError::Invalid("mutation replay cursor"))
}

fn start_cursor(session: &RestoreSession, plan: &Plan, latest: &MutationLogRoot) -> Result<Cursor> {
    let (previous, position) = match &plan.cached {
        Some(s) => (s.accepted.clone(), s.position.clone()),
        None => (
            plan.base.clone(),
            latest
                .anchor
                .position()
                .map_err(RestoreError::native_proof)?,
        ),
    };
    let cursor = Cursor {
        version: 2,
        head: plan.head.clone(),
        remaining: plan.links,
        previous,
        position,
        entry: 0,
        part: 0,
    };
    save_cursor(session, &cursor).map(|()| cursor)
}

async fn replay(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    mut cursor: Cursor,
) -> Result<run_restore::RunRestoreGuard> {
    local::ensure_directory(&session.root.join("replay-files"))?;
    local::sync_dir(&session.root)?;
    while cursor.remaining > 0 {
        replay_checkpoint(session, client, decryption, latest, &mut cursor).await?;
    }
    if cursor.previous != *session.accepted() || cursor.position != latest.through {
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
    cursor: &mut Cursor,
) -> Result<()> {
    let idx = cursor.remaining - 1;
    let accepted: AcceptedBackup = local::read_json(&link_path(session, idx), 256 * 1024)?;
    let ancestor = session.ancestor(accepted.clone())?;
    let (inventory, bytes) = load_closure(&ancestor, client).await?;
    let key = &decryption.content;
    let root = MutationLogRoot::decode(&bytes, key, &accepted.begin)
        .map_err(|_| RestoreError::Invalid("replayed checkpoint root"))?;
    if (&root.base, &root.anchor, &root.recovery)
        != (&latest.base, &latest.anchor, &latest.recovery)
        || root.previous != cursor.previous
        || cursor.entry > root.entries.len()
    {
        return Err(RestoreError::Invalid("replayed checkpoint predecessor"));
    }
    let pos = root.entries[..cursor.entry]
        .iter()
        .try_fold(root.from.clone(), |p, e| e.position_after(&p))
        .map_err(|_| RestoreError::Invalid("saved complete replay position"))?;
    if cursor.position != pos {
        return Err(RestoreError::Invalid("saved complete replay position"));
    }
    local::download_checkpoint(&ancestor, client, decryption, &root, inventory, idx).await?;
    let data = session.root.join("home/data");
    let replay = run_restore::MutationReplay::open(&data, key, &root.anchor)
        .map_err(|_| RestoreError::Invalid("private native replay admission"))?;
    while cursor.entry < root.entries.len() {
        let entry = &root.entries[cursor.entry];
        if cursor.part > entry.parts.len() {
            return Err(RestoreError::Invalid("saved replay part"));
        }
        while cursor.part < entry.parts.len() {
            let part = &entry.parts[cursor.part];
            let rel = log_path(&root.anchor.epoch, entry.sequence, part.role, &part.run_id);
            let pfile = session.root.join("replay-files").join(rel);
            replay
                .part(entry, cursor.part, &pfile)
                .map_err(|_| RestoreError::Invalid("original native mutation part replay"))?;
            cursor.part += 1;
            save_cursor(session, cursor)?;
        }
        cursor.position = entry
            .position_after(&cursor.position)
            .map_err(|_| RestoreError::Invalid("complete replay batch"))?;
        cursor.entry += 1;
        cursor.part = 0;
        save_cursor(session, cursor)?;
    }
    if replay.frontiers() != root.through.frontiers || cursor.position != root.through {
        return Err(RestoreError::Invalid("complete checkpoint frontier"));
    }
    release_checkpoint(session, &root)?;
    cursor.remaining = idx;
    cursor.previous = accepted;
    cursor.entry = 0;
    cursor.part = 0;
    save_cursor(session, cursor)?;
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
