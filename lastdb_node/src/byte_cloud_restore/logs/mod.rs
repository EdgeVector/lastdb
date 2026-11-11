//! Restore one fixed accepted root: a private snapshot followed by whole native batches.
mod plan;

use super::{
    inventory::Inventory, local, native, recipient::GuestReplayCache, ByteBackupReader,
    RecoveryDecryption, RestoreError, RestoreReport, RestoreSession, Result, VerifiedNativeContext,
};
use crate::byte_cloud_log::MutationLogRoot;
use cloud_backup_protocol::v2::AcceptedBackup;
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::{
    byte_backup::ClientRoot, mutation_log::LogPosition, run_restore,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;

pub(super) const MAX_CHAIN: u32 = 32768;

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

struct BaseSnapshot {
    session: RestoreSession,
    root: ClientRoot,
    inventory: Inventory,
}

pub(super) async fn restore(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    identity_seed: &[u8; 32],
    latest: MutationLogRoot,
) -> Result<RestoreReport> {
    decryption.validate_account(&latest.recovery.account_public_key)?;
    let plan = plan::prepare(session, client, decryption, &latest, None).await?;
    let base = load_base(session, client, decryption, &latest).await?;
    if let Some(report) = VerifiedNativeContext::new(session, &base.root.native_root).published()? {
        return Ok(report);
    }
    let cursor = if let Some(cursor) = load_cursor(session, &plan)? {
        cursor
    } else {
        stage_base(base, client, decryption).await?;
        start_cursor(session, &plan, &latest)?
    };
    let proof = replay(session, client, decryption, &latest, cursor).await?;
    let root = proof.native_root().map_err(RestoreError::native_proof)?;
    let (files, bytes) = proof.totals().map_err(RestoreError::native_proof)?;
    VerifiedNativeContext::new(session, &root).finish_run(
        identity_seed,
        super::NativeInventory { files, bytes },
        &proof,
    )
}

pub(super) async fn stage_guest(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: MutationLogRoot,
    cache: Option<&GuestReplayCache<'_>>,
) -> Result<(run_restore::RunRestoreGuard, LogPosition)> {
    decryption.validate_account(&latest.recovery.account_public_key)?;
    let plan = plan::prepare(session, client, decryption, &latest, cache).await?;
    let cursor = if let Some(cursor) = load_cursor(session, &plan)? {
        cursor
    } else {
        if let Some(start) = &plan.cached {
            let cache = cache.ok_or(RestoreError::Invalid("cached mutation predecessor"))?;
            seed_from_cache(session, decryption, &latest, cache, &start.position)?;
        } else {
            let base = load_base(session, client, decryption, &latest).await?;
            stage_base(base, client, decryption).await?;
        }
        start_cursor(session, &plan, &latest)?
    };
    let proof = replay(session, client, decryption, &latest, cursor).await?;
    Ok((proof, latest.through))
}

fn seed_from_cache(
    session: &RestoreSession,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    cache: &GuestReplayCache<'_>,
    position: &LogPosition,
) -> Result<()> {
    let data = session.root.join("home/data");
    let key = decryption.content_key();
    let reader = run_restore::RunSnapshotReader::open(cache.data, key, cache.native_root)
        .map_err(RestoreError::native_proof)?;
    reader
        .seed_replay(&data, &latest.anchor, position)
        .map_err(RestoreError::native_proof)?;
    local::prepare_native_ephemera(&data)?;
    run_restore::install_empty_cas_home(&data, key).map_err(RestoreError::native_proof)
}

async fn load_base(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
) -> Result<BaseSnapshot> {
    let base = session.ancestor(latest.base.clone())?;
    let (inventory, bytes) = super::load_closure(&base, client).await?;
    let root = super::snapshot_closure(&base, decryption, &bytes)?;
    let profile = &root.run;
    if root.recovery != latest.recovery
        || profile.main.home_id != latest.anchor.main_home
        || profile.main.frontier.get() != latest.anchor.frontiers.main
        || profile.cas.is_some()
        || !base.accepted().begin.dependencies().is_empty()
    {
        return Err(RestoreError::Invalid("mutation exact snapshot boundary"));
    }
    Ok(BaseSnapshot {
        session: base,
        root,
        inventory,
    })
}

async fn stage_base(
    base: BaseSnapshot,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
) -> Result<()> {
    let total = super::runs::stage(
        &base.session,
        client,
        decryption,
        &base.root,
        base.inventory,
    )
    .await?;
    let proof = super::runs::verify_stage(&base.session, decryption, &base.root)?;
    if proof.totals().map_err(RestoreError::native_proof)? != (total.files, total.bytes) {
        return Err(RestoreError::Invalid("mutation snapshot proof"));
    }
    Ok(())
}

fn cursor_path(session: &RestoreSession) -> PathBuf {
    session.root.join("chain/cursor.json")
}

fn load_cursor(session: &RestoreSession, plan: &plan::Plan) -> Result<Option<Cursor>> {
    let path = cursor_path(session);
    if !path.try_exists()? {
        return Ok(None);
    }
    let cursor: Cursor = local::read_json(&path, 256 * 1024)?;
    if cursor.version != 2 || cursor.head != plan.head || cursor.remaining > plan.links {
        return Err(RestoreError::Invalid("mutation replay cursor"));
    }
    Ok(Some(cursor))
}

fn start_cursor(
    session: &RestoreSession,
    plan: &plan::Plan,
    latest: &MutationLogRoot,
) -> Result<Cursor> {
    let (previous, position) = match &plan.cached {
        Some(start) => (start.accepted.clone(), start.position.clone()),
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
    plan::save(session, &cursor_path(session), &cursor)?;
    Ok(cursor)
}

async fn replay(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    mut cursor: Cursor,
) -> Result<run_restore::RunRestoreGuard> {
    let replay_files = session.root.join("replay-files");
    match fs::DirBuilder::new().mode(0o700).create(&replay_files) {
        Ok(()) => File::open(&session.root)?.sync_all()?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    local::directory(&replay_files)?;
    while cursor.remaining > 0 {
        replay_checkpoint(session, client, decryption, latest, &mut cursor).await?;
    }
    verify_complete(session, decryption, latest, &cursor)
}

fn verify_complete(
    session: &RestoreSession,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    cursor: &Cursor,
) -> Result<run_restore::RunRestoreGuard> {
    if cursor.previous != *session.accepted() || cursor.position != latest.through {
        return Err(RestoreError::Invalid("mutation complete selected prefix"));
    }
    let replay = run_restore::MutationReplay::open(
        &session.root.join("home/data"),
        decryption.content_key(),
        &latest.anchor,
    )
    .map_err(|_| RestoreError::Invalid("final native replay admission"))?;
    replay
        .settle()
        .map_err(|_| RestoreError::Invalid("final native replay compaction"))?;
    drop(replay);
    run_restore::verify_replayed(
        &session.root.join("home/data"),
        &latest.anchor,
        &latest.through,
        decryption.content_key(),
    )
    .map_err(RestoreError::native_proof)
}

async fn replay_checkpoint(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    cursor: &mut Cursor,
) -> Result<()> {
    let accepted: AcceptedBackup =
        local::read_json(&plan::link_path(session, cursor.remaining - 1), 256 * 1024)?;
    let ancestor = session.ancestor(accepted.clone())?;
    let (inventory, bytes) = super::load_closure(&ancestor, client).await?;
    let root = MutationLogRoot::decode(&bytes, decryption.content_key(), &accepted.begin)
        .map_err(|_| RestoreError::Invalid("replayed checkpoint root"))?;
    if root.base != latest.base
        || root.anchor != latest.anchor
        || root.recovery != latest.recovery
        || root.previous != cursor.previous
        || cursor.entry > root.entries.len()
    {
        return Err(RestoreError::Invalid("replayed checkpoint predecessor"));
    }
    let mut position = root.from.clone();
    for entry in &root.entries[..cursor.entry] {
        position = entry
            .position_after(&position)
            .map_err(|_| RestoreError::Invalid("saved complete replay position"))?;
    }
    if cursor.position != position {
        return Err(RestoreError::Invalid("saved complete replay position"));
    }
    plan::download_checkpoint(
        &ancestor,
        client,
        decryption,
        &root,
        inventory,
        cursor.remaining - 1,
    )
    .await?;
    let replay = run_restore::MutationReplay::open(
        &session.root.join("home/data"),
        decryption.content_key(),
        &root.anchor,
    )
    .map_err(|_| RestoreError::Invalid("private native replay admission"))?;
    while cursor.entry < root.entries.len() {
        let entry = &root.entries[cursor.entry];
        if cursor.part > entry.parts.len() {
            return Err(RestoreError::Invalid("saved replay part"));
        }
        while cursor.part < entry.parts.len() {
            let part = &entry.parts[cursor.part];
            let path = session.root.join("replay-files").join(
                fold_db::storage::disk_pack::mutation_log::log_path(
                    &root.anchor.epoch,
                    entry.sequence,
                    part.role,
                    &part.run_id,
                ),
            );
            replay
                .part(entry, cursor.part, &path)
                .map_err(|_| RestoreError::Invalid("original native mutation part replay"))?;
            cursor.part += 1;
            plan::save(session, &cursor_path(session), cursor)?;
        }
        cursor.position = entry
            .position_after(&cursor.position)
            .map_err(|_| RestoreError::Invalid("complete replay batch"))?;
        cursor.entry += 1;
        cursor.part = 0;
        plan::save(session, &cursor_path(session), cursor)?;
    }
    if replay.frontiers() != root.through.frontiers || cursor.position != root.through {
        return Err(RestoreError::Invalid("complete checkpoint frontier"));
    }
    release_checkpoint(session, &root)?;
    cursor.remaining -= 1;
    cursor.previous = accepted;
    cursor.entry = 0;
    cursor.part = 0;
    plan::save(session, &cursor_path(session), cursor)?;
    replay
        .settle()
        .map_err(|_| RestoreError::Invalid("complete checkpoint native compaction"))?;
    Ok(())
}

fn release_checkpoint(session: &RestoreSession, root: &MutationLogRoot) -> Result<()> {
    let mut parents = BTreeSet::new();
    for entry in &root.entries {
        for part in &entry.parts {
            let name = fold_db::storage::disk_pack::mutation_log::log_path(
                &root.anchor.epoch,
                entry.sequence,
                part.role,
                &part.run_id,
            );
            let path = session.root.join("replay-files").join(&name);
            match fs::symlink_metadata(&path) {
                Ok(meta) if meta.is_file() && meta.len() == part.bytes => fs::remove_file(&path)?,
                Ok(_) => return Err(RestoreError::Invalid("mutation source release identity")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            parents.insert(
                path.parent()
                    .ok_or(RestoreError::Invalid("mutation source parent"))?
                    .to_path_buf(),
            );
            session
                .remove_spool_file(&session.root.join("entries").join(native::path_key(&name)))?;
        }
    }
    for parent in parents {
        File::open(parent)?.sync_all()?;
    }
    File::open(session.root.join("entries"))?.sync_all()?;
    File::open(session.root.join("replay-files"))?.sync_all()?;
    plan::clear_cache(session)
}
