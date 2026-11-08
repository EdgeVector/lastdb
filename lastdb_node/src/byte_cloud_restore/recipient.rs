//! Stage one granted backup without creating an owner home.
use super::session::{acquire_native_work, RestoreMode};
use super::{
    closure, keys::RecoveryDecryption, reader_error, runs, ByteBackupReader, RestoreError,
    RestoreSession, Result,
};
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::v2::publication::HeadReadResult;
use cloud_backup_protocol::v2::AcceptedBackup;
use cloud_backup_protocol::{Digest, RecoveryDescriptor};
use std::fs;
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

impl RestoreSession {
    /// Select the current independent snapshot authorized by a guest permission.
    pub async fn begin_guest(destination: &Path, reader: &ByteBackupReader) -> Result<Self> {
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
        Self::begin_recipient(destination, accepted)
    }

    /// Reserve a private workspace for an independent accepted snapshot.
    pub fn begin_recipient(destination: &Path, accepted: AcceptedBackup) -> Result<Self> {
        require_independent(&accepted)?;
        Self::begin_with_mode(destination, accepted, RestoreMode::Recipient)
    }

    /// Verify and stage an exact granted snapshot. Supply the content key and
    /// publisher public key from the authenticated publication invitation.
    /// The returned path is private staging data, without an owner identity,
    /// credentials, or restore receipt.
    pub async fn stage_recipient(
        &mut self,
        reader: &ByteBackupReader,
        content_key: Zeroizing<[u8; 32]>,
        publisher_public_key: Digest,
    ) -> Result<PathBuf> {
        let accepted = self.accepted();
        if self.selection.mode != RestoreMode::Recipient
            || (reader.recipient_job() != Some(accepted.job())
                && reader.guest_stream() != Some(&accepted.job().stream))
        {
            return Err(RestoreError::Invalid("recipient restore session"));
        }
        require_independent(accepted)?;
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
        let root = closure::snapshot(self, &decryption, &bytes)?;
        let total = runs::stage(self, reader, &decryption, &root, inventory).await?;
        let proof = runs::verify_stage(self, &decryption, &root)?;
        let data = self.root.join("home/data");
        proof
            .validate_context(&data, &root.native_root)
            .map_err(|_| RestoreError::Invalid("verified recipient stage"))?;
        if proof
            .totals()
            .map_err(|_| RestoreError::Invalid("verified recipient totals"))?
            != (total.files, total.bytes)
        {
            return Err(RestoreError::Invalid("verified recipient inventory"));
        }
        self.require_unactivated()?;
        Ok(data)
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
