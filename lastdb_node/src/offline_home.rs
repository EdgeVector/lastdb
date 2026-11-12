//! Primary-home guard and descriptor-relative private home primitives.
use crate::backup_runtime::{BackupError, Result as BackupResult};
use crate::cloud::is_private_file;
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::fs::{File, Metadata};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use zeroize::Zeroizing;

const MAX_BYTES: u64 = 16 * 1024;
const SAFE: OFlags = OFlags::NOFOLLOW.union(OFlags::CLOEXEC);
pub const DIR_FLAGS: OFlags = OFlags::RDONLY.union(OFlags::DIRECTORY).union(SAFE);

/// Refuse `path` when it is, or lies under, the primary home `~/.lastdb`.
pub fn refuse_primary(path: &Path) -> Result<(), String> {
    let user = PathBuf::from(std::env::var_os("HOME").ok_or("HOME unset")?);
    let abs = canonicalize_lenient(path);
    if abs.starts_with(canonicalize_lenient(
        &user.join(crate::profile::paths::LASTDB_DIR),
    )) {
        return Err(format!(
            "refusing primary path {}: use a home outside ~/.lastdb",
            abs.display()
        ));
    }
    Ok(())
}

fn canonicalize_lenient(path: &Path) -> PathBuf {
    let (mut tail, mut cur) = (Vec::new(), path);
    while std::fs::canonicalize(cur).is_err() {
        let (Some(parent), Some(name)) = (cur.parent(), cur.file_name()) else {
            return path.to_path_buf();
        };
        tail.push(name.to_os_string());
        cur = parent;
    }
    let base = std::fs::canonicalize(cur).unwrap_or_else(|_| path.to_path_buf());
    tail.iter().rev().fold(base, |acc, part| acc.join(part))
}

fn eq_fd(a: &File, b: &File) -> io::Result<bool> {
    let (x, y) = (a.metadata()?, b.metadata()?);
    Ok(x.dev() == y.dev() && x.ino() == y.ino())
}

pub fn require_absolute(path: &Path) -> Result<(), String> {
    (path.is_absolute()
        && path
            .components()
            .all(|p| matches!(p, Component::RootDir | Component::Normal(_))))
    .then_some(())
    .ok_or_else(|| "credential-home paths must be absolute without parent components".into())
}

pub fn open_directory(path: &Path) -> Result<File, String> {
    open_directory_excluding(path, None)
}

pub fn open_directory_excluding(path: &Path, excluded: Option<&File>) -> Result<File, String> {
    require_absolute(path)?;
    let check = |cur: &File| -> Result<(), String> {
        match excluded.map(|ex| eq_fd(cur, ex)).transpose() {
            Ok(Some(true)) => {
                Err("destination parent must not contain the source home or its aliases".into())
            }
            Ok(_) => Ok(()),
            Err(_) => Err("cannot inspect directory".into()),
        }
    };
    let root = fs::open("/", DIR_FLAGS, Mode::empty()).map_err(|_| "cannot open directory root")?;
    let mut dir = File::from(root);
    check(&dir)?;
    for part in path.components() {
        if let Component::Normal(name) = part {
            dir = openat(&dir, name, DIR_FLAGS, 0)
                .map_err(|_| "directory path must exist without symlink components")?;
            check(&dir)?;
        }
    }
    Ok(dir)
}

pub fn require_owned_directory(dir: &File) -> Result<(), String> {
    let m = dir.metadata().map_err(|_| "cannot inspect source home")?;
    (m.uid() == unsafe { libc::geteuid() } && m.mode() & 0o022 == 0)
        .then_some(())
        .ok_or_else(|| "source home must be owner-held and not writable by other users".into())
}

pub fn require_mode_0700_directory(dir: &File, error: &'static str) -> Result<(), String> {
    let m = dir.metadata().map_err(|_| error)?;
    (m.uid() == unsafe { libc::geteuid() } && m.mode() & 0o777 == 0o700)
        .then_some(())
        .ok_or_else(|| error.to_owned())
}

fn is_private_0600(meta: &Metadata, limit: Option<u64>) -> bool {
    is_private_file(meta) && meta.mode() & 0o777 == 0o600 && limit.is_none_or(|n| meta.len() <= n)
}

pub fn require_private_file(
    file: &File,
    limit: Option<u64>,
    error: &'static str,
) -> Result<(), String> {
    is_private_0600(&file.metadata().map_err(|_| error)?, limit)
        .then_some(())
        .ok_or_else(|| error.to_owned())
}

pub fn entry_present(dir: &File, name: &str) -> Result<bool, String> {
    match fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => Ok(true),
        Err(rustix::io::Errno::NOENT) => Ok(false),
        Err(_) => Err("cannot inspect credential-home entry".into()),
    }
}

pub fn open_private_at(
    dir: &File,
    name: &str,
    flags: OFlags,
    mode: Mode,
    error: &'static str,
) -> Result<File, String> {
    let flags = flags | SAFE;
    let file = File::from(fs::openat(dir, name, flags, mode).map_err(|_| error)?);
    if flags.contains(OFlags::EXCL) {
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| error)?;
    }
    require_private_file(&file, None, error).map(|()| file)
}

pub fn dir_entry_names(dir: &File, error: &'static str) -> Result<Vec<Vec<u8>>, String> {
    let mut names = Vec::new();
    for entry in fs::Dir::read_from(dir).map_err(|_| error)? {
        let name = entry.map_err(|_| error)?.file_name().to_bytes().to_vec();
        if !matches!(name.as_slice(), b"." | b"..") {
            names.push(name);
        }
    }
    Ok(names)
}

pub fn read_private_file(dir: &File, name: &str, limit: u64) -> Result<Vec<u8>, String> {
    read_private_file_bound(dir, name, limit, None)
}

pub fn read_private_file_bound(
    dir: &File,
    name: &str,
    limit: u64,
    expected: Option<&File>,
) -> Result<Vec<u8>, String> {
    const ERR: &str = "credential file must be a bounded owner-only regular file";
    let flags = OFlags::RDONLY | OFlags::NONBLOCK;
    let file = open_private_at(dir, name, flags, Mode::empty(), ERR)?;
    if expected
        .map(|h| eq_fd(&file, h))
        .transpose()
        .map_err(|_| "cannot inspect credential file")?
        == Some(false)
    {
        return Err("recovery credential file changed during preparation".into());
    }
    require_private_file(&file, Some(limit), ERR)?;
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read credential file")?;
    (bytes.len() as u64 <= limit)
        .then_some(bytes)
        .ok_or_else(|| "credential file exceeds the supported size".into())
}

pub fn same_inode<P: rustix::path::Arg>(dir: &File, name: P, held: &File) -> bool {
    fs::fstat(held).is_ok_and(|h| {
        fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)
            .is_ok_and(|e| e.st_dev == h.st_dev && e.st_ino == h.st_ino)
    })
}

pub(crate) struct Directory {
    path: PathBuf,
    file: File,
    max_bytes: u64,
}

fn openat(
    dir: &File,
    name: impl rustix::path::Arg,
    flags: OFlags,
    mode: fs::RawMode,
) -> io::Result<File> {
    fs::openat(dir, name, flags, Mode::from_bits_truncate(mode))
        .map(File::from)
        .map_err(Into::into)
}

impl Directory {
    pub fn open(path: &Path) -> BackupResult<Self> {
        Self::open_with_limit(path, MAX_BYTES)
    }

    pub(crate) fn open_with_limit(path: &Path, max_bytes: u64) -> BackupResult<Self> {
        require_absolute(path).map_err(|_| BackupError::Invalid)?;
        let file = open_directory(path).map_err(|_| BackupError::Disk)?;
        require_owned_directory(&file).map_err(|_| BackupError::Identity)?;
        Ok(Self {
            path: path.to_owned(),
            file,
            max_bytes,
        })
    }

    pub fn check(&self) -> BackupResult<()> {
        eq_fd(&self.file, &Self::open(&self.path)?.file)?
            .then_some(())
            .ok_or(BackupError::Identity)
    }

    pub(crate) fn open_lock(&self, name: &str) -> BackupResult<File> {
        self.check()?;
        let file = openat(
            &self.file,
            name,
            OFlags::RDWR | OFlags::CREATE | OFlags::NONBLOCK | SAFE,
            0o600,
        )?;
        is_private_0600(&file.metadata()?, Some(MAX_BYTES))
            .then_some(file)
            .ok_or(BackupError::Identity)
    }

    pub(crate) fn remove_exact(&self, name: &str, expected: &[u8]) -> BackupResult<()> {
        if self.read(name)?.as_deref().map(Vec::as_slice) != Some(expected) {
            return Err(BackupError::Identity);
        }
        fs::unlinkat(&self.file, name, AtFlags::empty()).map_err(io::Error::from)?;
        Ok(self.file.sync_all()?)
    }

    pub fn read(&self, name: &str) -> BackupResult<Option<Zeroizing<Vec<u8>>>> {
        self.check()?;
        let flags = OFlags::RDONLY | OFlags::NONBLOCK | SAFE;
        let mut file = match fs::openat(&self.file, name, flags, Mode::empty()) {
            Ok(fd) => File::from(fd),
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(e) => return Err(io::Error::from(e).into()),
        };
        let before = file.metadata()?;
        if !is_private_0600(&before, Some(self.max_bytes)) {
            return Err(BackupError::Identity);
        }
        let mut bytes = Zeroizing::new(Vec::new());
        (&mut file)
            .take(self.max_bytes.saturating_add(1))
            .read_to_end(&mut bytes)?;
        let (after, cur) = (
            file.metadata()?,
            openat(&self.file, name, flags, 0)?.metadata()?,
        );
        if bytes.len() as u64 != before.len()
            || stamp(&before) != stamp(&after)
            || stamp(&after) != stamp(&cur)
        {
            return Err(BackupError::Identity);
        }
        self.check().map(|()| Some(bytes))
    }

    pub fn publish(&self, name: &str, expected: Option<&[u8]>, bytes: &[u8]) -> BackupResult<()> {
        if bytes.len() as u64 > self.max_bytes {
            return Err(BackupError::Limit);
        }
        let current = self.read(name)?;
        let slice = current.as_deref().map(Vec::as_slice);
        if slice == Some(bytes) {
            return Ok(());
        }
        if slice != expected {
            return Err(BackupError::Identity);
        }
        self.check()?;
        let temp = format!(".byte-backup-connection-{}.pending", uuid::Uuid::new_v4());
        let mut file = openat(
            &self.file,
            temp.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | SAFE,
            0o600,
        )?;
        let res: BackupResult<()> = (|| {
            file.write_all(bytes)?;
            file.sync_all()?;
            self.check()?;
            let rflags = expected.map_or(fs::RenameFlags::NOREPLACE, |_| fs::RenameFlags::empty());
            fs::renameat_with(&self.file, temp.as_str(), &self.file, name, rflags)
                .map_err(io::Error::from)?;
            Ok(())
        })();
        if res.is_err() {
            let _ = fs::unlinkat(&self.file, temp.as_str(), AtFlags::empty());
        }
        res?;
        self.file.sync_all()?;
        (self.read(name)?.as_deref().map(Vec::as_slice) == Some(bytes))
            .then_some(())
            .ok_or(BackupError::Identity)
    }
}

fn stamp(m: &Metadata) -> [i128; 6] {
    [
        m.dev() as i128,
        m.ino() as i128,
        m.len() as i128,
        (i128::from(m.uid()) << 32) | i128::from(m.mode()) | ((m.nlink() as i128) << 64),
        (i128::from(m.mtime()) << 32) ^ i128::from(m.mtime_nsec()),
        (i128::from(m.ctime()) << 32) ^ i128::from(m.ctime_nsec()),
    ]
}
