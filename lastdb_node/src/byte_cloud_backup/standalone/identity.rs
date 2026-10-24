//! Read-only binding to one protected account identity and its actual home.
use super::super::{BackupError, Result};
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::{AccountId, Digest, RecoveryDescriptor};
use fold_db::security::Ed25519KeyPair;
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

pub(in crate::byte_cloud_backup) struct Identity {
    home: PathBuf,
    directory: File,
    file: File,
    seed: Zeroizing<[u8; 32]>,
    account: AccountId,
    recovery: RecoveryDescriptor,
}

impl Identity {
    pub(in crate::byte_cloud_backup) fn open(home: &Path) -> Result<Self> {
        if !home.is_absolute() || !std::fs::symlink_metadata(home)?.is_dir() {
            return Err(BackupError::Identity);
        }
        let home = home.canonicalize()?;
        let directory = File::from(
            fs::open(
                &home,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| BackupError::Identity)?,
        );
        let metadata = directory.metadata()?;
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o022 != 0 {
            return Err(BackupError::Identity);
        }
        let file = File::from(
            fs::openat(
                &directory,
                crate::host::IDENTITY_KEY_FILE,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|_| BackupError::Identity)?,
        );
        let seed = read_seed(&file)?;
        let pair =
            Ed25519KeyPair::from_secret_key(seed.as_ref()).map_err(|_| BackupError::Identity)?;
        let value = Self {
            home,
            directory,
            file,
            seed,
            account: AccountId::try_from(
                lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())
                    .map_err(|_| BackupError::Identity)?,
            )?,
            recovery: RecoveryDescriptor {
                profile: RecoveryProfile::MiniPortableV1,
                account_public_key: Digest::try_from(fold_db::hex::hex_lower(
                    pair.public_key_bytes(),
                ))?,
            },
        };
        value.check()?;
        Ok(value)
    }

    pub(in crate::byte_cloud_backup) fn check(&self) -> Result<()> {
        let held = self.directory.metadata()?;
        let current = std::fs::symlink_metadata(&self.home)?;
        if !current.is_dir() || held.dev() != current.dev() || held.ino() != current.ino() {
            return Err(BackupError::Identity);
        }
        let held = fs::fstat(&self.file).map_err(|_| BackupError::Identity)?;
        let current = fs::statat(
            &self.directory,
            crate::host::IDENTITY_KEY_FILE,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| BackupError::Identity)?;
        if held.st_dev != current.st_dev
            || held.st_ino != current.st_ino
            || read_seed(&self.file)?.as_ref() != self.seed.as_ref()
        {
            return Err(BackupError::Identity);
        }
        Ok(())
    }

    pub(in crate::byte_cloud_backup) fn home(&self) -> &Path {
        &self.home
    }
    pub(in crate::byte_cloud_backup) fn seed(&self) -> &[u8; 32] {
        &self.seed
    }
    pub(in crate::byte_cloud_backup) fn account(&self) -> &AccountId {
        &self.account
    }
    pub(in crate::byte_cloud_backup) fn recovery(&self) -> &RecoveryDescriptor {
        &self.recovery
    }
}

fn read_seed(file: &File) -> Result<Zeroizing<[u8; 32]>> {
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o777 != 0o600
        || metadata.nlink() != 1
        || metadata.len() != 32
    {
        return Err(BackupError::Identity);
    }
    let mut seed = Zeroizing::new([0; 32]);
    file.read_exact_at(seed.as_mut(), 0)?;
    let mut extra = [0; 1];
    if file.read_at(&mut extra, 32)? != 0 || file.metadata()?.len() != 32 {
        return Err(BackupError::Identity);
    }
    Ok(seed)
}
