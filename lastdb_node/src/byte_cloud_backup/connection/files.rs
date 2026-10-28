//! Descriptor-relative private publication. A record is published after its credentials.
use super::{BackupError, Result};
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::fs::{File, Metadata};
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use zeroize::Zeroizing;

const MAX_BYTES: u64 = 16 * 1024;

pub(super) struct Directory {
    path: PathBuf,
    file: File,
}

impl Directory {
    pub fn open(path: &Path) -> Result<Self> {
        if !path.is_absolute()
            || path
                .components()
                .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
        {
            return Err(BackupError::Invalid);
        }
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let mut file =
            File::from(fs::open("/", flags, Mode::empty()).map_err(std::io::Error::from)?);
        for part in path.components() {
            if let Component::Normal(name) = part {
                file = File::from(
                    fs::openat(&file, name, flags, Mode::empty()).map_err(std::io::Error::from)?,
                );
            }
        }
        let info = file.metadata()?;
        if info.uid() != unsafe { libc::geteuid() } || info.mode() & 0o022 != 0 {
            return Err(BackupError::Identity);
        }
        Ok(Self {
            path: path.to_owned(),
            file,
        })
    }

    pub fn check(&self) -> Result<()> {
        let current = Self::open(&self.path)?;
        let held = self.file.metadata()?;
        let actual = current.file.metadata()?;
        if held.dev() != actual.dev() || held.ino() != actual.ino() {
            return Err(BackupError::Identity);
        }
        Ok(())
    }

    pub fn read(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        self.check()?;
        let descriptor = match fs::openat(
            &self.file,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        ) {
            Ok(file) => file,
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(error) => return Err(std::io::Error::from(error).into()),
        };
        let mut file = File::from(descriptor);
        let before = file.metadata()?;
        private(&before)?;
        let mut bytes = Zeroizing::new(Vec::new());
        (&mut file).take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
        let after = file.metadata()?;
        let current = File::from(
            fs::openat(
                &self.file,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(std::io::Error::from)?,
        )
        .metadata()?;
        if bytes.len() as u64 != before.len() || !same(&before, &after) || !same(&after, &current) {
            return Err(BackupError::Identity);
        }
        self.check()?;
        Ok(Some(bytes))
    }

    pub fn publish(&self, name: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() as u64 > MAX_BYTES {
            return Err(BackupError::Limit);
        }
        if let Some(current) = self.read(name)? {
            return if current.as_slice() == bytes {
                Ok(())
            } else {
                Err(BackupError::Identity)
            };
        }
        self.check()?;
        let temporary = format!(".byte-backup-connection-{}.pending", uuid::Uuid::new_v4());
        let mut file = File::from(
            fs::openat(
                &self.file,
                temporary.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_bits_truncate(0o600),
            )
            .map_err(std::io::Error::from)?,
        );
        let result: Result<()> = (|| {
            file.write_all(bytes)?;
            file.sync_all()?;
            self.check()?;
            fs::renameat_with(
                &self.file,
                temporary.as_str(),
                &self.file,
                name,
                fs::RenameFlags::NOREPLACE,
            )
            .map_err(std::io::Error::from)?;
            Ok(())
        })();
        if result.is_err() {
            // Only an unpublished private temporary can remain after this error.
            let _ = fs::unlinkat(&self.file, temporary.as_str(), AtFlags::empty());
        }
        result?;
        self.file.sync_all()?;
        if self.read(name)?.as_deref().map(Vec::as_slice) != Some(bytes) {
            return Err(BackupError::Identity);
        }
        Ok(())
    }
}

fn private(info: &Metadata) -> Result<()> {
    if !info.is_file()
        || info.uid() != unsafe { libc::geteuid() }
        || info.mode() & 0o777 != 0o600
        || info.nlink() != 1
        || info.len() > MAX_BYTES
    {
        return Err(BackupError::Identity);
    }
    Ok(())
}

fn same(left: &Metadata, right: &Metadata) -> bool {
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.uid() == right.uid()
        && left.mode() == right.mode()
        && left.nlink() == right.nlink()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}
