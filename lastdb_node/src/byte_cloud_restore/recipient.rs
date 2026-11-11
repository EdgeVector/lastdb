//! Guest staging and verified restored-home publication.
use super::session::{acquire_native_work, RestoreMode};
use super::{
    load_closure, local, logs, reader_error, runs, snapshot_closure, ByteBackupReader,
    NativeInventory, RecoveryDecryption, RestoreError, RestoreReport, RestoreSession, Result,
    VerifiedNativeContext, RESTORE_RECEIPT_NAME, RESTORE_RECEIPT_VERSION,
};
use crate::byte_cloud_log::MutationLogRoot;
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::v2::publication::{Head, HeadReadResult};
use cloud_backup_protocol::v2::AcceptedBackup;
use cloud_backup_protocol::{Digest, Id, NativeRoot, RecoveryDescriptor};
use fold_db::storage::disk_pack::{mutation_log::LogPosition, run_restore::RunRestoreGuard};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

/// Read-only previous state while the cache holds its slot lock.
pub(crate) struct GuestReplayCache<'a> {
    pub(crate) data: &'a Path,
    pub(crate) head: &'a Head,
    pub(crate) native_root: &'a NativeRoot,
    pub(crate) through: Option<&'a LogPosition>,
}

type RecipientStage<T> = (PathBuf, T, Option<LogPosition>);

impl RestoreSession {
    pub(crate) async fn guest_accepted(reader: &ByteBackupReader) -> Result<AcceptedBackup> {
        let stream = reader
            .guest_stream()
            .ok_or(RestoreError::Invalid("guest reader"))?;
        let accepted = match reader
            .guest_head()
            .await
            .map_err(|error| reader_error(reader, error))?
        {
            HeadReadResult::Ok { response } => response
                .accepted
                .ok_or(RestoreError::Source("share has no published backup"))?,
            HeadReadResult::Error { error } => return Err(reader_error(reader, error.into())),
        };
        if &accepted.job().stream != stream {
            return Err(RestoreError::Invalid("guest stream"));
        }
        Ok(accepted)
    }

    pub(crate) fn begin_guest_at(destination: &Path, accepted: AcceptedBackup) -> Result<Self> {
        Self::begin_with_mode(destination, accepted, RestoreMode::Recipient)
    }

    pub(crate) fn begin_managed_guest(slot: &Path, accepted: AcceptedBackup) -> Result<Self> {
        let invalid = || RestoreError::Invalid("managed guest slot");
        let entry = slot.parent().ok_or_else(invalid)?;
        let registry = entry.parent().ok_or_else(invalid)?;
        if !matches!(
            slot.file_name().and_then(|name| name.to_str()),
            Some("snapshot-0" | "snapshot-1")
        ) || entry
            .file_name()
            .and_then(|name| name.to_str())
            .is_none_or(|name| Id::try_from(name.to_owned()).is_err())
            || registry.file_name().and_then(|name| name.to_str())
                != Some(super::MANAGED_SHARE_DIRECTORY)
            || slot.canonicalize()? != slot
        {
            return Err(invalid());
        }
        for path in [registry, entry, slot] {
            local::directory(path)?;
        }
        Self::begin_in(&slot.join("unactivated"), accepted, RestoreMode::Recipient)
    }

    pub(crate) async fn stage_guest_recovery(
        &mut self,
        reader: &ByteBackupReader,
        content_key: Zeroizing<[u8; 32]>,
        publisher_public_key: Digest,
        cache: Option<&GuestReplayCache<'_>>,
    ) -> Result<RecipientStage<NativeRoot>> {
        let accepted = self.accepted();
        if self.selection.mode != RestoreMode::Recipient
            || reader.guest_stream() != Some(&accepted.job().stream)
        {
            return Err(RestoreError::Invalid("recipient restore session"));
        }
        let recovery = RecoveryDescriptor {
            profile: RecoveryProfile::MiniPortableV1,
            account_public_key: publisher_public_key,
        };
        recovery.validate_for(&accepted.job().stream.scope)?;
        let decryption =
            RecoveryDecryption::for_publication(content_key, &recovery.account_public_key);
        let _admission = acquire_native_work()?;
        self.require_unactivated()?;
        self.authorize_read(reader).await?;
        let (inventory, bytes) = load_closure(self, reader).await?;
        let data = self.root.join("home/data");
        let (proof, through) = if self.accepted().begin.dependencies().is_empty() {
            let root = snapshot_closure(self, &decryption, &bytes)?;
            let total = runs::stage(self, reader, &decryption, &root, inventory).await?;
            let proof = runs::verify_stage(self, &decryption, &root)?;
            proof
                .validate_context(&data, &root.native_root)
                .map_err(RestoreError::native_proof)?;
            if proof.totals().map_err(RestoreError::native_proof)? != (total.files, total.bytes) {
                return Err(RestoreError::Invalid("verified recipient inventory"));
            }
            (proof, None)
        } else {
            let root =
                MutationLogRoot::decode(&bytes, decryption.content_key(), &self.accepted().begin)
                    .map_err(|_| RestoreError::Invalid("recipient mutation log root"))?;
            drop(inventory);
            let (proof, through) =
                logs::stage_guest(self, reader, &decryption, root, cache).await?;
            (proof, Some(through))
        };
        self.require_unactivated()?;
        let native_root = proof.native_root().map_err(RestoreError::native_proof)?;
        Ok((data, native_root, through))
    }

    fn require_unactivated(&self) -> Result<()> {
        let home = self.root.join("home");
        for path in [
            self.selection.destination.clone(),
            home.join(crate::identity::IDENTITY_KEY_FILE),
            crate::cloud::cloud_credentials_path(&home),
        ] {
            match fs::symlink_metadata(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
                Ok(_) => return Err(RestoreError::Invalid("recipient stage already activated")),
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u8,
    session_id: uuid::Uuid,
    report: RestoreReport,
}

pub(super) fn published_context(
    session: &VerifiedNativeContext<'_>,
) -> Result<Option<RestoreReport>> {
    let destination = session.destination;
    match fs::symlink_metadata(destination) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(meta) if !meta.is_dir() => return Err(RestoreError::Invalid("published destination")),
        Ok(_) => {}
    }
    let receipt: Receipt = local::read_json(&destination.join(RESTORE_RECEIPT_NAME), 64 * 1024)?;
    if receipt.version != RESTORE_RECEIPT_VERSION
        || receipt.session_id != session.session_id
        || receipt.report.commit_sha256 != session.commit_sha256.as_str()
        || receipt.report.destination != *destination
    {
        return Err(RestoreError::Invalid("published receipt"));
    }
    sync_publish_parents(session.root, destination)?;
    local::atomic_json(&session.root.join("result.json"), &receipt.report)?;
    Ok(Some(receipt.report))
}

pub(super) fn finish_run_context(
    session: &VerifiedNativeContext<'_>,
    identity_seed: &[u8; 32],
    inventory: NativeInventory,
    proof: &RunRestoreGuard,
) -> Result<RestoreReport> {
    proof
        .validate_context(&session.data_root(), session.native_root)
        .map_err(|_| RestoreError::Invalid("verified run publication"))?;
    let (files, bytes) = proof
        .totals()
        .map_err(|_| RestoreError::Invalid("verified run totals"))?;
    if files != inventory.files || bytes != inventory.bytes {
        return Err(RestoreError::Invalid("verified run inventory"));
    }
    let home = session.root.join("home");
    install_identity(&home, identity_seed)?;
    if crate::cloud::cloud_credentials_path(&home).try_exists()? {
        return Err(RestoreError::Invalid("restore cloud configuration"));
    }
    let report = RestoreReport {
        commit_sha256: session.commit_sha256.as_str().into(),
        native_files: inventory.files,
        native_bytes: inventory.bytes,
        verified_records: proof.verified_records(),
        destination: session.destination.to_path_buf(),
    };
    local::atomic_json(
        &home.join(RESTORE_RECEIPT_NAME),
        &Receipt {
            version: RESTORE_RECEIPT_VERSION,
            session_id: session.session_id,
            report: report.clone(),
        },
    )?;
    activate_owned_home(session.root, session.destination)?;
    local::atomic_json(&session.root.join("result.json"), &report)?;
    Ok(report)
}

pub(crate) fn activate_owned_home(root: &Path, destination: &Path) -> Result<()> {
    let home = root.join("home");
    sync_owned_tree(&home, 0)?;
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        &home,
        rustix::fs::CWD,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(std::io::Error::from)?;
    sync_publish_parents(root, destination)
}

pub(crate) fn sync_publish_parents(root: &Path, destination: &Path) -> Result<()> {
    File::open(root)?.sync_all()?;
    File::open(
        destination
            .parent()
            .ok_or(RestoreError::Invalid("destination parent"))?,
    )?
    .sync_all()?;
    Ok(())
}

fn install_identity(home: &Path, identity_seed: &[u8; 32]) -> Result<()> {
    let path = home.join(crate::identity::IDENTITY_KEY_FILE);
    if path.try_exists()? {
        let mut file = local::owner_file(&path, false)?;
        let mut seed = Zeroizing::new(Vec::new());
        Read::by_ref(&mut file).take(33).read_to_end(&mut seed)?;
        if seed.len() > 32 || !identity_seed.starts_with(&seed) {
            return Err(RestoreError::Invalid("restore identity"));
        }
        if seed.len() < 32 {
            file.seek(SeekFrom::Start(0))?;
            file.write_all(identity_seed)?;
        }
    } else {
        crate::identity::write_seed(home, identity_seed)
            .map_err(|_| RestoreError::Invalid("restore identity flush"))?;
    }
    File::open(path)?.sync_all()?;
    File::open(home)?.sync_all()?;
    Ok(())
}

pub(crate) fn sync_owned_tree(root: &Path, depth: usize) -> Result<()> {
    if depth > 12 {
        return Err(RestoreError::Limit("destination depth"));
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            sync_owned_tree(&entry.path(), depth + 1)?;
        } else if !kind.is_file() {
            return Err(RestoreError::Invalid("destination member type"));
        }
    }
    File::open(root)?.sync_all()?;
    Ok(())
}
