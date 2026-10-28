//! Restore one fixed accepted root: a private snapshot followed by whole native batches.
mod checkpoint;
mod plan;
use super::{
    local, ByteBackupClient, RecoveryKeys, RestoreError, RestoreReport, RestoreSession, Result,
    VerifiedNativeContext,
};
use crate::byte_cloud_log::MutationLogRoot;
use cloud_backup_protocol::v2::AcceptedBackup;
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::{
    byte_backup::ClientRoot, mutation_log::LogPosition, run_restore,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;

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

pub(super) async fn restore(
    session: &RestoreSession,
    client: &ByteBackupClient,
    keys: &RecoveryKeys,
    latest: MutationLogRoot,
) -> Result<RestoreReport> {
    keys.validate_account(&latest.recovery.account_public_key)?;
    let plan = plan::prepare(session, client, keys, &latest).await?;
    let base = session.ancestor(plan.base.clone())?;
    let (inventory, bytes) = super::closure::load(&base, client).await?;
    let snapshot = ClientRoot::decode(&bytes, keys.content_key(), &base.accepted().begin)
        .map_err(|_| RestoreError::Invalid("mutation base snapshot"))?;
    let profile = snapshot
        .run
        .as_ref()
        .ok_or(RestoreError::Invalid("mutation base profile"))?;
    if snapshot.recovery != latest.recovery
        || profile.main.home_id != latest.anchor.main_home
        || profile.cas.home_id != latest.anchor.cas_home
        || profile.main.frontier.get() != latest.anchor.frontiers.main
        || profile.cas.frontier.get() != latest.anchor.frontiers.cas
    {
        return Err(RestoreError::Invalid("mutation exact snapshot boundary"));
    }
    if let Some(report) = context(session, &snapshot.native_root).published()? {
        return Ok(report);
    }
    let cursor_path = session.root.join("chain/cursor.json");
    let mut cursor: Cursor = if cursor_path.try_exists()? {
        local::read_json(&cursor_path, 256 * 1024)?
    } else {
        let total = super::runs::stage(&base, client, keys, &snapshot, inventory).await?;
        let proof = super::runs::verify_stage(&base, keys, &snapshot)?;
        if proof
            .totals()
            .map_err(|_| RestoreError::Invalid("mutation snapshot proof"))?
            != (total.files, total.bytes)
        {
            return Err(RestoreError::Invalid("mutation snapshot proof"));
        }
        drop(proof);
        let cursor = Cursor {
            version: 1,
            head: plan.head.clone(),
            remaining: plan.links,
            previous: plan.base.clone(),
            position: latest
                .anchor
                .position()
                .map_err(|_| RestoreError::Invalid("mutation anchor"))?,
            entry: 0,
            part: 0,
        };
        plan::save(session, &cursor_path, &cursor)?;
        cursor
    };
    if cursor.version != 1 || cursor.head != plan.head || cursor.remaining > plan.links {
        return Err(RestoreError::Invalid("mutation replay cursor"));
    }
    for name in ["replay-files", "run-proof"] {
        let path = session.root.join(name);
        match fs::DirBuilder::new().mode(0o700).create(&path) {
            Ok(()) => File::open(&session.root)?.sync_all()?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        local::directory(&path)?;
    }
    while cursor.remaining > 0 {
        replay_checkpoint(session, client, keys, &latest, &mut cursor).await?;
    }
    if cursor.previous != *session.accepted() || cursor.position != latest.through {
        return Err(RestoreError::Invalid("mutation complete selected prefix"));
    }
    let proof = run_restore::verify_replayed(
        &session.root.join("home/data"),
        &latest.anchor,
        &latest.through,
        keys.content_key(),
        &session.root.join("run-proof"),
    )
    .map_err(|_| RestoreError::Invalid("complete replayed native proof"))?;
    let root = proof
        .native_root()
        .map_err(|_| RestoreError::Invalid("replayed native root"))?;
    let (files, bytes) = proof
        .totals()
        .map_err(|_| RestoreError::Invalid("replayed native totals"))?;
    context(session, &root).finish_run(
        keys,
        super::NativeInventory {
            files,
            bytes,
            dependencies: 0,
        },
        &proof,
    )
}

async fn replay_checkpoint(
    session: &RestoreSession,
    client: &ByteBackupClient,
    keys: &RecoveryKeys,
    latest: &MutationLogRoot,
    cursor: &mut Cursor,
) -> Result<()> {
    let accepted: AcceptedBackup =
        local::read_json(&plan::link_path(session, cursor.remaining - 1), 256 * 1024)?;
    let ancestor = session.ancestor(accepted.clone())?;
    let (inventory, bytes) = super::closure::load(&ancestor, client).await?;
    let root = MutationLogRoot::decode(&bytes, keys.content_key(), &accepted.begin)
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
    checkpoint::download(&ancestor, client, keys, &root, inventory).await?;
    let replay = run_restore::MutationReplay::open(
        &session.root.join("home/data"),
        keys.content_key(),
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
            plan::save(session, &session.root.join("chain/cursor.json"), cursor)?;
        }
        cursor.position = entry
            .position_after(&cursor.position)
            .map_err(|_| RestoreError::Invalid("complete replay batch"))?;
        cursor.entry += 1;
        cursor.part = 0;
        plan::save(session, &session.root.join("chain/cursor.json"), cursor)?;
    }
    if replay.frontiers() != root.through.frontiers || cursor.position != root.through {
        return Err(RestoreError::Invalid("complete checkpoint frontier"));
    }
    drop(replay);
    checkpoint::release(session, &root)?;
    cursor.remaining -= 1;
    cursor.previous = accepted;
    cursor.entry = 0;
    cursor.part = 0;
    plan::save(session, &session.root.join("chain/cursor.json"), cursor)
}

fn context<'a>(
    session: &'a RestoreSession,
    root: &'a cloud_backup_protocol::NativeRoot,
) -> VerifiedNativeContext<'a> {
    VerifiedNativeContext {
        root: &session.root,
        destination: &session.selection.destination,
        session_id: session.selection.session_id,
        commit_sha256: &session.accepted().commit_sha256,
        native_root: root,
        receipt_name: "byte-cloud-restore-receipt.json",
        receipt_version: 2,
    }
}
