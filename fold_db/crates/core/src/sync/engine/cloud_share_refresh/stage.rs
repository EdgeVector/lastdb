//! Bounded encrypted seeds are all verified before receiver writes begin.

use super::*;
use crate::sync::snapshot::{Snapshot, SnapshotValueEncoding};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;

pub(super) struct StagedShare {
    pub target: SyncTarget,
    pub path: PathBuf,
    pub ciphertext_sha256: String,
    pub snapshot_sha256: String,
    pub last_seq: u64,
    pub writer_frontier: BTreeMap<String, u64>,
}

pub(super) fn consistent_notice(
    shares: &[(SyncTarget, Option<ShareReadNotice>)],
) -> SyncResult<ShareReadNotice> {
    let first = shares
        .first()
        .and_then(|(_, notice)| notice.as_ref())
        .ok_or_else(|| held("an active share has no current signed read grant"))?;
    for (_, notice) in shares {
        if !notice.as_ref().is_some_and(|notice| {
            notice.account == first.account
                && notice.generation == first.generation
                && notice.data_generation == first.data_generation
                && notice.operation_id == first.operation_id
        }) {
            return Err(held(
                "the sender share notices do not name one open generation",
            ));
        }
    }
    Ok(first.clone())
}

pub(super) async fn download(
    engine: &SyncEngine,
    shares: &[(SyncTarget, Option<ShareReadNotice>)],
    notice: &ShareReadNotice,
) -> SyncResult<Vec<StagedShare>> {
    let peer = engine
        .cloud_purge_peer()
        .ok_or_else(|| held("the receiver has no durable retention directory"))?;
    let directory = peer.directory().join("foreign-share-seeds");
    std::fs::create_dir_all(&directory)?;
    let meta = std::fs::symlink_metadata(&directory)?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err(held("the receiver seed directory is unsafe"));
    }
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    let mut staged = Vec::new();
    for (target, _) in shares {
        let mut target = target.clone();
        let binding = target
            .read_binding
            .as_mut()
            .ok_or_else(|| held("the foreign read binding disappeared"))?;
        binding.generation = notice.generation;
        binding.data_generation = notice.data_generation;
        crate::sync::cloud_share::validate_grant(&target)?;
        let url = engine
            .auth
            .presign_snapshot_download_for_target(&target, "latest.enc")
            .await?;
        let bytes = engine
            .s3
            .download_limited(
                &url,
                Some(crate::sync::snapshot::cloud_purge::MAX_SHARE_SNAPSHOT_BYTES + 4096),
            )
            .await?
            .ok_or_else(|| held("the replacement share snapshot is absent"))?;
        let snapshot = Snapshot::unseal(&bytes, &target.crypto).await?;
        validate(&snapshot, &target.prefix)?;
        let path = directory.join(format!("{}.enc", uuid::Uuid::new_v4()));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        staged.push(StagedShare {
            target,
            path,
            ciphertext_sha256: crate::hex::sha256_hex(&bytes),
            snapshot_sha256: crate::hex::sha256_hex(serde_json::to_vec(&snapshot)?),
            last_seq: snapshot.last_seq,
            writer_frontier: snapshot.writer_frontier,
        });
    }
    std::fs::File::open(&directory)?.sync_all()?;
    Ok(staged)
}

pub(super) fn validate(snapshot: &Snapshot, root: &str) -> SyncResult<()> {
    if snapshot.value_encoding != SnapshotValueEncoding::Logical || snapshot.device_id.is_empty() {
        return Err(held("the replacement share is not a logical photograph"));
    }
    snapshot.cloud_purge_share_bytes()?;
    if snapshot.writer_frontier.keys().any(String::is_empty)
        || snapshot
            .writer_frontier
            .values()
            .copied()
            .max()
            .unwrap_or(0)
            != snapshot.last_seq
    {
        return Err(held(
            "the replacement share lacks its exact writer frontier",
        ));
    }
    let mut namespaces = std::collections::BTreeSet::new();
    for namespace in &snapshot.namespaces {
        if !namespaces.insert(&namespace.name)
            || namespace.name.starts_with("sync_")
            || crate::sync::snapshot::snapshot_should_skip_namespace(&namespace.name)
            || matches!(
                namespace.name.as_str(),
                "share_subscriptions"
                    | "share_rules"
                    | "molecule_keys"
                    | "db_catalog"
                    | "org_sync_targets"
            )
        {
            if namespace.entries.is_empty() {
                continue;
            }
            return Err(held(
                "the replacement share contains protected or duplicate metadata",
            ));
        }
        for entry in &namespace.entries {
            use base64::{engine::general_purpose::STANDARD, Engine as _};
            let key = STANDARD
                .decode(&entry.key)
                .map_err(|_| held("the share snapshot key is invalid"))?;
            STANDARD
                .decode(&entry.value)
                .map_err(|_| held("the share snapshot value is invalid"))?;
            if namespace.name == "cas_blobs" {
                let key =
                    std::str::from_utf8(&key).map_err(|_| held("the share file key is invalid"))?;
                if !key.strip_prefix("sha256:").is_some_and(|hash| {
                    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                }) {
                    return Err(held("the share file identity is invalid"));
                }
            } else {
                crate::sync::cloud_share::rewrite_key(&key, root, "from:validation")?;
            }
        }
    }
    Ok(())
}

pub(super) fn binding(
    sender: &str,
    notice: &ShareReadNotice,
    roots: &[String],
    staged: &[StagedShare],
) -> SyncResult<ReceiverBinding> {
    let snapshots = staged
        .iter()
        .map(|item| (item.target.prefix.clone(), item.snapshot_sha256.clone()))
        .collect();
    let binding = ReceiverBinding {
        format_version: 1,
        sender_hash: sender.into(),
        account: notice.account.clone(),
        generation: notice.generation,
        data_generation: notice.data_generation,
        operation_id: notice.operation_id.clone(),
        storage_prefix: crate::sync::cloud_share::receiver_prefix(
            sender,
            notice.data_generation,
            roots,
            &snapshots,
        )?,
        share_roots: roots.to_vec(),
        snapshots,
        writer_frontiers: staged
            .iter()
            .map(|item| (item.target.prefix.clone(), item.writer_frontier.clone()))
            .collect(),
    };
    binding.validate()?;
    Ok(binding)
}
