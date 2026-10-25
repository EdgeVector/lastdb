//! Rebuild native files only after all four client streams pass their closure.
use super::{dependencies, inventory::Inventory, local, record_bridge, record_plan, record_stream};
use super::{
    ByteBackupClient, RecoveryKeys, RestoreError, RestoreReport, RestoreSession, Result,
    VerifiedNativeContext,
};
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::byte_backup::{
    import_record_backup, ClientRoot, ReassembledRecordStream, StoredRecordProfile,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    version: u8,
    commit_sha256: Digest,
    root_receipt_id: Digest,
    profile_sha256: Digest,
}

pub(super) async fn restore(
    session: &RestoreSession,
    client: &ByteBackupClient,
    keys: &RecoveryKeys,
    root: &ClientRoot,
    mut inventory: Inventory,
) -> Result<RestoreReport> {
    let profile = root
        .records
        .as_ref()
        .ok_or(RestoreError::Invalid("record profile"))?;
    let plans =
        record_plan::collect(session, client, root, &mut inventory, keys.content_key()).await?;
    let target = session.root.join("record-import");
    let ready = Ready {
        version: 1,
        commit_sha256: session.accepted().commit_sha256.clone(),
        root_receipt_id: session.accepted().root_receipt_id.clone(),
        profile_sha256: cloud_backup_protocol::bytes_sha256(
            &serde_json::to_vec(profile)
                .map_err(|_| RestoreError::Invalid("record profile encoding"))?,
        ),
    };
    let marker = session.root.join("record-ready.json");
    if marker.try_exists()? {
        validate_ready(&marker, &ready)?;
    } else {
        if target.try_exists()? {
            return Err(RestoreError::Invalid(
                "partial record import requires fresh session",
            ));
        }
        require_empty_physical(session)?;
        crate::native_cloud_restore::check_capacity(&session.root, profile.receipt.body_bytes)?;
        pipeline(session, client, keys, profile, &plans, &inventory, &target).await?;
        local::atomic_json(&marker, &ready)?;
    }
    drop(plans);
    drop(inventory);
    if target.try_exists()? {
        verify_import(&target, profile)?;
        require_empty_physical(session)?;
        private_import_directory(&target)?;
        fs::rename(&target, session.physical())?;
        File::open(session.root.join("home/data"))?.sync_all()?;
        File::open(&session.root)?.sync_all()?;
    } else {
        verify_import(&session.physical(), profile)?;
    }
    finish(session, keys, root)
}

async fn pipeline(
    session: &RestoreSession,
    client: &ByteBackupClient,
    keys: &RecoveryKeys,
    profile: &StoredRecordProfile,
    plans: &[record_plan::StreamPlan],
    inventory: &Inventory,
    target: &Path,
) -> Result<()> {
    if plans.len() != 4 {
        return Err(RestoreError::Invalid("record stream count"));
    }
    let (metadata, metadata_input) = record_bridge::channel();
    let (r2, r2_input) = record_bridge::channel();
    let (b2, b2_input) = record_bridge::channel();
    let (sequence, sequence_input) = record_bridge::channel();
    let owned = profile.clone();
    let target = target.to_path_buf();
    let importer = tokio::task::spawn_blocking(move || {
        let mut input = ReassembledRecordStream::new(
            &owned,
            Box::new(metadata_input),
            Box::new(r2_input),
            Box::new(b2_input),
            Box::new(sequence_input),
        )
        .map_err(|_| RestoreError::Invalid("record stream reassembly"))?;
        import_record_backup(&target, &mut input, &owned)
            .map_err(|_| RestoreError::Invalid("complete native record import"))
    });
    let results = futures::future::join_all([
        record_stream::feed(
            session,
            client,
            inventory,
            &plans[0],
            metadata,
            keys.content_key(),
        ),
        record_stream::feed(
            session,
            client,
            inventory,
            &plans[1],
            r2,
            keys.content_key(),
        ),
        record_stream::feed(
            session,
            client,
            inventory,
            &plans[2],
            b2,
            keys.content_key(),
        ),
        record_stream::feed(
            session,
            client,
            inventory,
            &plans[3],
            sequence,
            keys.content_key(),
        ),
    ])
    .await;
    let imported = importer
        .await
        .map_err(|_| RestoreError::Invalid("record importer task"));
    for result in results {
        result?;
    }
    imported??;
    Ok(())
}

fn validate_ready(path: &Path, expected: &Ready) -> Result<()> {
    let ready: Ready = local::read_json(
        path,
        cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES,
    )?;
    if ready.version != expected.version
        || ready.commit_sha256 != expected.commit_sha256
        || ready.root_receipt_id != expected.root_receipt_id
        || ready.profile_sha256 != expected.profile_sha256
    {
        return Err(RestoreError::Invalid("saved complete record import"));
    }
    Ok(())
}

fn verify_import(physical: &Path, profile: &StoredRecordProfile) -> Result<()> {
    let home = laststore::disk_pack::home::DiskHome::open(physical)
        .map_err(|_| RestoreError::Invalid("rebuilt record home"))?;
    let actual = laststore::disk_pack::home::record_stream::export(&home, &mut std::io::sink())
        .map_err(|_| RestoreError::Invalid("rebuilt stored record verification"))?;
    if actual != profile.receipt {
        return Err(RestoreError::Invalid("rebuilt stored record receipt"));
    }
    Ok(())
}

fn require_empty_physical(session: &RestoreSession) -> Result<()> {
    let physical = session.physical();
    local::directory(&physical)?;
    if fs::read_dir(physical)?.next().transpose()?.is_some() {
        return Err(RestoreError::Invalid("record physical scaffold"));
    }
    Ok(())
}

fn private_import_directory(path: &Path) -> Result<()> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)?;
    let metadata = directory.metadata()?;
    if !metadata.is_dir() || metadata.uid() != unsafe { libc::geteuid() } {
        return Err(RestoreError::Invalid("owned record import directory"));
    }
    directory.set_permissions(fs::Permissions::from_mode(0o700))?;
    directory.sync_all()?;
    Ok(())
}

fn finish(
    session: &RestoreSession,
    keys: &RecoveryKeys,
    root: &ClientRoot,
) -> Result<RestoreReport> {
    let (native_root, totals, proofs) =
        crate::native_cloud_restore::rebuild_record_snapshot(&session.physical())?;
    let profile = root
        .records
        .as_ref()
        .ok_or(RestoreError::Invalid("record profile"))?;
    if native_root.frontier.get() != profile.header.frontier()
        || native_root.required_features.get() != profile.header.required_features()
    {
        return Err(RestoreError::Invalid("rebuilt record frontier or features"));
    }
    let context = VerifiedNativeContext {
        root: &session.root,
        destination: &session.selection.destination,
        session_id: session.selection.session_id,
        commit_sha256: &session.accepted().commit_sha256,
        native_root: &native_root,
        receipt_name: "byte-cloud-restore-receipt.json",
        receipt_version: 2,
    };
    let proofs: BTreeMap<_, _> = proofs
        .into_iter()
        .map(|proof| (proof.path.clone(), proof))
        .collect();
    dependencies::prepare(&context, &root.recovery.account_public_key)?;
    let atoms = context.verify_records(keys.content_key(), 0)?;
    context.verify_metadata(keys.content_key())?;
    context.finish(keys, totals, atoms, |path| {
        let proof = proofs
            .get(path)
            .ok_or(RestoreError::Invalid("rebuilt native proof"))?;
        Ok(super::NativeFileProof {
            path: proof.path.clone(),
            sha256: proof.sha256.clone(),
            bytes: proof.bytes,
        })
    })
}
