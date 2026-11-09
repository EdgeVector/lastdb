//! Stage one granted backup without creating an owner home.
use super::session::{acquire_native_work, RestoreMode};
use super::{
    closure, keys::RecoveryDecryption, logs, reader_error, runs, ByteBackupReader, RestoreError,
    RestoreSession, Result,
};
use crate::byte_cloud_log::MutationLogRoot;
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::v2::publication::{Head, HeadReadResult};
use cloud_backup_protocol::v2::AcceptedBackup;
use cloud_backup_protocol::{Digest, NativeRoot, RecoveryDescriptor};
use fold_db::storage::disk_pack::{mutation_log::LogPosition, run_restore::RunRestoreGuard};
use std::fs;
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

pub use fold_db::storage::disk_pack::run_restore::{StagedRecord, StagedRecordQuery};

/// Read-only previous state while the cache holds its slot lock.
pub(crate) struct GuestReplayCache<'a> {
    pub(crate) data: &'a Path,
    pub(crate) head: &'a Head,
    pub(crate) native_root: &'a NativeRoot,
    pub(crate) through: Option<&'a LogPosition>,
}

type RecipientStage<T> = (PathBuf, T, Option<LogPosition>);

impl RestoreSession {
    /// Select the current accepted backup authorized by a guest permission.
    pub async fn begin_guest(destination: &Path, reader: &ByteBackupReader) -> Result<Self> {
        Self::begin_guest_at(destination, Self::guest_accepted(reader).await?)
    }

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

    /// Reserve a private workspace for an independent accepted snapshot.
    pub fn begin_recipient(destination: &Path, accepted: AcceptedBackup) -> Result<Self> {
        require_independent(&accepted)?;
        Self::begin_with_mode(destination, accepted, RestoreMode::Recipient)
    }

    /// Verify and stage an exact granted backup. Supply the content key and
    /// publisher public key from the authenticated publication invitation.
    /// The returned path is private staging data, without an owner identity,
    /// credentials, or restore receipt.
    pub async fn stage_recipient(
        &mut self,
        reader: &ByteBackupReader,
        content_key: Zeroizing<[u8; 32]>,
        publisher_public_key: Digest,
    ) -> Result<PathBuf> {
        let (data, (), _) = self
            .with_verified_recipient_stage(
                reader,
                content_key,
                publisher_public_key,
                None,
                |_, _| Ok(()),
            )
            .await?;
        Ok(data)
    }

    pub(crate) async fn stage_guest_recovery(
        &mut self,
        reader: &ByteBackupReader,
        content_key: Zeroizing<[u8; 32]>,
        publisher_public_key: Digest,
        cache: Option<GuestReplayCache<'_>>,
    ) -> Result<RecipientStage<NativeRoot>> {
        self.with_verified_recipient_stage(
            reader,
            content_key,
            publisher_public_key,
            cache,
            |proof, _| proof.native_root().map_err(RestoreError::native_proof),
        )
        .await
    }

    /// Stage the exact guest recovery point and read one primary key from its held root.
    pub async fn stage_guest_record(
        &mut self,
        reader: &ByteBackupReader,
        content_key: Zeroizing<[u8; 32]>,
        publisher_public_key: Digest,
        query: &StagedRecordQuery<'_>,
    ) -> Result<(PathBuf, Option<StagedRecord>)> {
        if reader.guest_stream().is_none() {
            return Err(RestoreError::Invalid("guest reader"));
        }
        let (data, record, _) = self
            .with_verified_recipient_stage(
                reader,
                content_key,
                publisher_public_key,
                None,
                |proof, key| {
                    proof
                        .read_staged_record(query, key)
                        .map_err(RestoreError::native_proof)
                },
            )
            .await?;
        Ok((data, record))
    }

    async fn with_verified_recipient_stage<T>(
        &mut self,
        reader: &ByteBackupReader,
        content_key: Zeroizing<[u8; 32]>,
        publisher_public_key: Digest,
        cache: Option<GuestReplayCache<'_>>,
        read: impl FnOnce(&RunRestoreGuard, &[u8; 32]) -> Result<T>,
    ) -> Result<RecipientStage<T>> {
        let accepted = self.accepted();
        if self.selection.mode != RestoreMode::Recipient
            || (reader.recipient_job() != Some(accepted.job())
                && reader.guest_stream() != Some(&accepted.job().stream))
        {
            return Err(RestoreError::Invalid("recipient restore session"));
        }
        if reader.guest_stream().is_none() {
            require_independent(accepted)?;
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
        let (inventory, bytes) = closure::load(self, reader).await?;
        let data = self.root.join("home/data");
        let (proof, through) = if self.accepted().begin.dependencies().is_empty() {
            let root = closure::snapshot(self, &decryption, &bytes)?;
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
        let result = read(&proof, decryption.content_key())?;
        Ok((data, result, through))
    }

    fn require_unactivated(&self) -> Result<()> {
        let home = self.root.join("home");
        for path in [
            self.selection.destination.clone(),
            home.join(lastdb_identity::IDENTITY_KEY_FILE),
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

fn require_independent(accepted: &AcceptedBackup) -> Result<()> {
    if !accepted.begin.dependencies().is_empty() {
        return Err(RestoreError::Invalid(
            "recipient snapshot has recovery dependencies",
        ));
    }
    Ok(())
}
