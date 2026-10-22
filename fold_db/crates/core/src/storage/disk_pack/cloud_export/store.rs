use super::catalog::ExportPage;
use super::state::{ExportRecord, PageDescriptor};
use super::{invalid, StorageResult};
use cloud_backup_protocol::{BackupJob, DecimalU64};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub(super) const PAGE_MAX: u64 = 2 * 1024 * 1024;
const RECORD_MAX: u64 = 256 * 1024;

pub(super) struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn open(native_home: &Path) -> StorageResult<Self> {
        // This sidecar is outside the exact generation inventory. Native disk
        // admission still counts its bytes beneath this selected home.
        let root = native_home.join("cloud-export-v1");
        directory(&root)?;
        Ok(Self { root })
    }

    pub fn load(&self) -> StorageResult<Option<ExportRecord>> {
        let path = self.root.join("job.json");
        if !path.try_exists()? {
            return Ok(None);
        }
        let record: ExportRecord = read_json(&path, RECORD_MAX)?;
        record.validate()?;
        Ok(Some(record))
    }

    pub fn save(&self, record: &ExportRecord) -> StorageResult<()> {
        record.validate()?;
        write_json(&self.root.join("job.json"), record, RECORD_MAX).map(|_| ())
    }

    pub fn spool_bytes(&self, record: &ExportRecord) -> StorageResult<u64> {
        let mut total = 0u64;
        let mut paths = vec![
            self.root.join("job.json"),
            self.root.join("job.pending"),
            self.root.join("accepted.json"),
            self.root.join("accepted.pending"),
        ];
        for index in 0..=record.pages.len() as u32 {
            let path = self.page_path(&record.begin.job, index)?;
            paths.push(path.with_extension("pending"));
            paths.push(path);
        }
        for path in paths {
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.is_file() => {
                    total = total
                        .checked_add(metadata.len())
                        .ok_or_else(|| invalid("native export spool count overflow"))?;
                }
                Ok(_) => return Err(invalid("native export spool path is not regular")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if total > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES {
            return Err(invalid("native export durable spool exceeds its limit"));
        }
        Ok(total)
    }

    pub fn accepted(
        &self,
    ) -> StorageResult<Option<cloud_backup_protocol::publication::AcceptedBackup>> {
        let path = self.root.join("accepted.json");
        if !path.try_exists()? {
            return Ok(None);
        }
        let backup: cloud_backup_protocol::publication::AcceptedBackup =
            read_json(&path, 16 * 1024)?;
        backup.validate().map_err(super::protocol)?;
        Ok(Some(backup))
    }

    pub fn save_accepted(
        &self,
        backup: &cloud_backup_protocol::publication::AcceptedBackup,
    ) -> StorageResult<()> {
        backup.validate().map_err(super::protocol)?;
        if let Some(previous) = self.accepted()? {
            if previous.job == backup.job && previous != *backup {
                return Err(invalid("saved accepted native export changed"));
            }
            if previous.job.stream == backup.job.stream
                && previous != *backup
                && previous.ordinal.get() >= backup.ordinal.get()
            {
                return Err(invalid("accepted native export ordinal moved backwards"));
            }
        }
        write_json(&self.root.join("accepted.json"), backup, 16 * 1024).map(|_| ())
    }

    fn page_path(&self, job: &BackupJob, index: u32) -> StorageResult<PathBuf> {
        let root = self.root.join(job.commit_id.as_str());
        Ok(root.join(format!("page-{index:04}.json")))
    }

    pub fn write_page(&self, page: &ExportPage) -> StorageResult<PageDescriptor> {
        page.validate()?;
        directory(&self.root.join(page.job.commit_id.as_str()))?;
        let (bytes, sha256) =
            write_json(&self.page_path(&page.job, page.page_index)?, page, PAGE_MAX)?;
        Ok(PageDescriptor {
            page_index: page.page_index,
            sha256,
            bytes: DecimalU64::new(bytes),
            objects: page.objects.len() as u64,
            files: page
                .objects
                .iter()
                .map(|object| object.files.len() as u64)
                .sum(),
        })
    }

    pub fn read_page(
        &self,
        job: &BackupJob,
        descriptor: &PageDescriptor,
    ) -> StorageResult<ExportPage> {
        let path = self.page_path(job, descriptor.page_index)?;
        let file = regular(&path)?;
        if file.metadata()?.len() != descriptor.bytes.get() {
            return Err(invalid("native export page length changed"));
        }
        let mut input = DigestReader {
            input: file,
            hash: Sha256::new(),
            bytes: 0,
            limit: PAGE_MAX,
        };
        let page: ExportPage = serde_json::from_reader(&mut input)
            .map_err(|error| invalid(&format!("native export page decode: {error}")))?;
        let digest = hex(input.hash.finalize().into());
        if digest != descriptor.sha256
            || input.bytes != descriptor.bytes.get()
            || page.job != *job
            || page.page_index != descriptor.page_index
            || page.objects.len() as u64 != descriptor.objects
            || page
                .objects
                .iter()
                .map(|object| object.files.len() as u64)
                .sum::<u64>()
                != descriptor.files
        {
            return Err(invalid("native export page identity changed"));
        }
        page.validate()?;
        Ok(page)
    }

    pub fn cleanup_pages(&self, record: &ExportRecord) -> StorageResult<()> {
        let root = self.root.join(record.begin.job.commit_id.as_str());
        if !root.try_exists()? {
            return Ok(());
        }
        directory(&root)?;
        // A crash after one page write can precede its descriptor barrier.
        for index in 0..=record.pages.len() as u32 {
            let path = self.page_path(&record.begin.job, index)?;
            for candidate in [&path, &path.with_extension("pending")] {
                match fs::remove_file(candidate) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        File::open(&root)?.sync_all()?;
        fs::remove_dir(root)?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

fn directory(path: &Path) -> StorageResult<()> {
    if !path.try_exists()? {
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new().mode(0o700).create(path)?;
        }
        #[cfg(not(unix))]
        fs::create_dir(path)?;
        File::open(
            path.parent()
                .ok_or_else(|| invalid("native export parent missing"))?,
        )?
        .sync_all()?;
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(invalid("native export state is not an owned directory"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(invalid("native export state directory must be private"));
        }
    }
    Ok(())
}

fn regular(path: &Path) -> StorageResult<File> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(invalid("native export state file is not regular"));
    }
    Ok(File::open(path)?)
}

fn read_json<T: DeserializeOwned>(path: &Path, maximum: u64) -> StorageResult<T> {
    let file = regular(path)?;
    if file.metadata()?.len() > maximum {
        return Err(invalid("native export state size exceeded"));
    }
    serde_json::from_reader(file.take(maximum + 1))
        .map_err(|error| invalid(&format!("native export state decode: {error}")))
}

fn write_json<T: Serialize>(
    path: &Path,
    value: &T,
    maximum: u64,
) -> StorageResult<(u64, cloud_backup_protocol::Digest)> {
    let temporary = path.with_extension("pending");
    match fs::symlink_metadata(&temporary) {
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            return Err(invalid("native export pending file is not regular"))
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&temporary)?;
    let mut output = DigestWriter {
        output: file,
        hash: Sha256::new(),
        bytes: 0,
        limit: maximum,
    };
    serde_json::to_writer(&mut output, value)
        .map_err(|error| invalid(&format!("native export state encode: {error}")))?;
    output.output.sync_all()?;
    let bytes = output.bytes;
    let digest = hex(output.hash.finalize().into());
    fs::rename(temporary, path)?;
    File::open(
        path.parent()
            .ok_or_else(|| invalid("native export state parent missing"))?,
    )?
    .sync_all()?;
    Ok((bytes, digest))
}

pub(super) fn hex(value: [u8; 32]) -> cloud_backup_protocol::Digest {
    cloud_backup_protocol::Digest::try_from(
        value
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    )
    .expect("SHA-256 always has canonical hexadecimal form")
}

struct DigestWriter {
    output: File,
    hash: Sha256,
    bytes: u64,
    limit: u64,
}
impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes + bytes.len() as u64 > self.limit {
            return Err(std::io::Error::other("native export state byte limit"));
        }
        let count = self.output.write(bytes)?;
        self.hash.update(&bytes[..count]);
        self.bytes += count as u64;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}

struct DigestReader {
    input: File,
    hash: Sha256,
    bytes: u64,
    limit: u64,
}
impl Read for DigestReader {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let maximum = bytes
            .len()
            .min(self.limit.saturating_add(1).saturating_sub(self.bytes) as usize);
        let count = self.input.read(&mut bytes[..maximum])?;
        self.bytes += count as u64;
        if self.bytes > self.limit {
            return Err(std::io::Error::other("native export state byte limit"));
        }
        self.hash.update(&bytes[..count]);
        Ok(count)
    }
}
