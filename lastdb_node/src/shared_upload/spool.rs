use super::{Result, UploadError};
use cloud_backup_protocol::shared::LogBatch;
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, limits};
use fold_db::portable_replay::MAX_LOG_BYTES;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const SEALED_BYTES_MAX: usize = MAX_LOG_BYTES + 64;

pub(super) struct Frozen {
    pub batch: LogBatch,
    pub sealed: Vec<u8>,
}

impl Frozen {
    fn validate(&self) -> Result<()> {
        self.batch.validate()?;
        if self.sealed.len() > SEALED_BYTES_MAX
            || self.sealed.len() as u64 != self.batch.segment.bytes.get()
            || bytes_sha256(&self.sealed) != self.batch.segment.sha256
        {
            return Err(UploadError::Invalid("frozen segment bytes"));
        }
        Ok(())
    }
}

pub(super) struct Spool {
    directory: PathBuf,
    _owner: File,
}

impl Spool {
    pub(super) fn open(home_data: &Path) -> Result<Self> {
        let directory = home_data.join("shared-upload-v1");
        match DirBuilder::new().mode(0o700).create(&directory) {
            Ok(()) => File::open(home_data)?.sync_all()?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let metadata = fs::symlink_metadata(&directory)?;
        if !metadata.is_dir() || !private(&metadata) {
            return Err(UploadError::Invalid("upload spool directory"));
        }
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(directory.join("owner.lock"))?;
        validate_file(&owner)?;
        owner
            .try_lock()
            .map_err(|_| UploadError::Invalid("upload spool is busy"))?;
        Ok(Self {
            directory,
            _owner: owner,
        })
    }

    pub(super) fn load(&self) -> Result<Option<Frozen>> {
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(self.directory.join("batch"))
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        validate_file(&file)?;
        let mut frame = [0; 4];
        file.read_exact(&mut frame)?;
        let metadata_len = u32::from_be_bytes(frame) as usize;
        if metadata_len as u64 > limits::REQUEST_JSON_MAX_BYTES {
            return Err(UploadError::Invalid("frozen metadata length"));
        }
        let mut metadata = vec![0; metadata_len];
        file.read_exact(&mut metadata)?;
        let batch: LogBatch = serde_json::from_slice(&metadata)
            .map_err(|_| UploadError::Invalid("frozen metadata"))?;
        let mut sealed = Vec::new();
        file.take((SEALED_BYTES_MAX + 1) as u64)
            .read_to_end(&mut sealed)?;
        let frozen = Frozen { batch, sealed };
        frozen.validate()?;
        Ok(Some(frozen))
    }

    pub(super) fn save(&self, frozen: &Frozen) -> Result<()> {
        frozen.validate()?;
        let metadata = canonical_bytes(&frozen.batch)?;
        let length = (metadata.len() as u32).to_be_bytes();
        let pending = self.directory.join("batch.pending");
        let bytes = (length.len() + metadata.len() + frozen.sealed.len()) as u64;
        let _space = laststore::space::reserve_space(
            &self.directory,
            &laststore::space::SpacePolicy::default(),
            bytes,
        )
        .map_err(|_| UploadError::Invalid("upload spool space"))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&pending)?;
        validate_file(&file)?;
        file.set_len(0)?;
        file.write_all(&length)?;
        file.write_all(&metadata)?;
        file.write_all(&frozen.sealed)?;
        file.sync_all()?;
        fs::rename(&pending, self.directory.join("batch"))?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    pub(super) fn release(&self) -> Result<()> {
        fs::remove_file(self.directory.join("batch"))?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }
}

fn private(metadata: &fs::Metadata) -> bool {
    metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o077 == 0
}

fn validate_file(file: &File) -> Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() || !private(&metadata) || metadata.nlink() != 1 {
        return Err(UploadError::Invalid("owned upload spool file"));
    }
    Ok(())
}
