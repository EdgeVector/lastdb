//! Descriptor-relative private home primitives.
use crate::profile::IDENTITY_KEY_FILE;
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use zeroize::Zeroizing;

const MAX_BYTES: u64 = 16 * 1024;
const SAFE: OFlags = OFlags::NOFOLLOW.union(OFlags::CLOEXEC);
const DISK_ERR: &str = "offline directory disk failure";
const CHANGED_ERR: &str = "offline directory identity changed";
pub const DIR_FLAGS: OFlags = OFlags::RDONLY.union(OFlags::DIRECTORY).union(SAFE);

type DirectoryResult<T> = io::Result<T>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DirInode(u64, u64);

impl DirInode {
    pub fn of_dir(path: &Path) -> io::Result<Self> {
        let meta = std::fs::symlink_metadata(path)?;
        if !meta.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged path is not a directory",
            ));
        }
        Ok(Self(meta.dev(), meta.ino()))
    }

    pub(crate) fn of_file(file: &File) -> io::Result<Self> {
        let meta = file.metadata()?;
        Ok(Self(meta.dev(), meta.ino()))
    }

    #[must_use]
    pub fn matches_dir(self, path: &Path) -> bool {
        Self::of_dir(path).is_ok_and(|actual| actual == self)
    }

    pub fn remove_dir_if_owned(self, path: &Path) {
        if self.matches_dir(path) {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

fn eq_fd(a: &File, b: &File) -> io::Result<bool> {
    Ok(DirInode::of_file(a)? == DirInode::of_file(b)?)
}

pub(crate) fn is_private_file(meta: &Metadata) -> bool {
    meta.is_file()
        && meta.uid() == unsafe { libc::geteuid() }
        && meta.mode() & 0o077 == 0
        && meta.nlink() == 1
}

pub fn read_private_named(
    path: &Path,
    max_bytes: u64,
    priv_err: &'static str,
    lim_err: &'static str,
) -> Result<Option<Zeroizing<Vec<u8>>>, &'static str> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(priv_err),
    };
    if !meta.is_file() {
        return Err(priv_err);
    }
    if meta.len() > max_bytes {
        return Err(lim_err);
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| priv_err)?;
    if !is_private_file(&file.metadata().map_err(|_| priv_err)?) {
        return Err(priv_err);
    }
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| priv_err)?;
    (bytes.len() as u64 <= max_bytes)
        .then_some(Some(bytes))
        .ok_or(lim_err)
}

fn decode_seed(raw: &[u8]) -> Result<Zeroizing<[u8; 32]>, String> {
    <[u8; 32]>::try_from(raw)
        .map(Zeroizing::new)
        .map_err(|_| "home identity.key must contain exactly 32 bytes".into())
}

pub(crate) fn read_home_identity_seed(home: &Path) -> Result<Option<Zeroizing<[u8; 32]>>, String> {
    const PRIV_ERR: &str = "home identity.key must be a private owner file";
    const LIM_ERR: &str = "home identity.key exceeds 32 bytes";
    read_private_named(&home.join(IDENTITY_KEY_FILE), 32, PRIV_ERR, LIM_ERR)
        .map_err(str::to_owned)?
        .map(|raw| decode_seed(&raw))
        .transpose()
}

/// The ids that the identity key of `home` derives: the personal database id and the node
/// key, each 64 lowercase hex characters. It only reads `identity.key`.
pub fn derived_identity(home: &Path) -> Result<(String, String), String> {
    let seed =
        read_home_identity_seed(home)?.ok_or_else(|| "the home has no identity.key".to_owned())?;
    let node_key = app_identity_crypto::SigningKey::from_bytes(&seed)
        .verifying_key()
        .to_bytes();
    Ok((
        crate::personal_identity::database_id(&seed),
        app_identity_crypto::hex_lower(&node_key),
    ))
}

pub fn read_identity_seed_at(dir: &File) -> Result<Zeroizing<[u8; 32]>, String> {
    decode_seed(&Zeroizing::new(read_private_file(
        dir,
        IDENTITY_KEY_FILE,
        32,
    )?))
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
    require_absolute(path)?;
    let mut dir = File::from(
        fs::open("/", DIR_FLAGS, Mode::empty()).map_err(|_| "cannot open directory root")?,
    );
    for part in path.components() {
        if let Component::Normal(name) = part {
            dir = openat(&dir, name, DIR_FLAGS, 0)
                .map_err(|_| "directory path must exist without symlink components")?;
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

fn require_private_file(
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
    const ERR: &str = "credential file must be a bounded owner-only regular file";
    let file = open_private_at(
        dir,
        name,
        OFlags::RDONLY | OFlags::NONBLOCK,
        Mode::empty(),
        ERR,
    )?;
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

pub struct Directory {
    path: PathBuf,
    file: File,
}

fn openat(
    dir: &File,
    name: impl rustix::path::Arg,
    flags: OFlags,
    mode: fs::RawMode,
) -> DirectoryResult<File> {
    disk(fs::openat(dir, name, flags, Mode::from_bits_truncate(mode)).map(File::from))
}

fn disk<T, E>(result: Result<T, E>) -> DirectoryResult<T> {
    result.map_err(|_| io::Error::other(DISK_ERR))
}

impl Directory {
    pub fn open(path: &Path) -> DirectoryResult<Self> {
        require_absolute(path).map_err(|_| io::Error::other("invalid offline directory path"))?;
        let file = disk(open_directory(path))?;
        require_owned_directory(&file).map_err(|_| io::Error::other(CHANGED_ERR))?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
        })
    }

    pub(crate) fn check(&self) -> DirectoryResult<()> {
        disk(eq_fd(&self.file, &Self::open(&self.path)?.file))?
            .then_some(())
            .ok_or_else(|| io::Error::other(CHANGED_ERR))
    }

    pub(crate) fn open_lock(&self, name: &str) -> DirectoryResult<File> {
        self.check()?;
        let file = openat(
            &self.file,
            name,
            OFlags::RDWR | OFlags::CREATE | OFlags::NONBLOCK | SAFE,
            0o600,
        )?;
        is_private_0600(&disk(file.metadata())?, Some(MAX_BYTES))
            .then_some(file)
            .ok_or_else(|| io::Error::other(CHANGED_ERR))
    }

    pub fn read(&self, name: &str) -> DirectoryResult<Option<Zeroizing<Vec<u8>>>> {
        self.check()?;
        let flags = OFlags::RDONLY | OFlags::NONBLOCK | SAFE;
        let mut file = match fs::openat(&self.file, name, flags, Mode::empty()) {
            Ok(fd) => File::from(fd),
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(_) => return Err(io::Error::other(DISK_ERR)),
        };
        let before = disk(file.metadata())?;
        if !is_private_0600(&before, Some(MAX_BYTES)) {
            return Err(io::Error::other(CHANGED_ERR));
        }
        let mut bytes = Zeroizing::new(Vec::new());
        disk(
            (&mut file)
                .take(MAX_BYTES.saturating_add(1))
                .read_to_end(&mut bytes),
        )?;
        let (after, cur) = (
            disk(file.metadata())?,
            disk(openat(&self.file, name, flags, 0)?.metadata())?,
        );
        if bytes.len() as u64 != before.len()
            || stamp(&before) != stamp(&after)
            || stamp(&after) != stamp(&cur)
        {
            return Err(io::Error::other(CHANGED_ERR));
        }
        self.check().map(|()| Some(bytes))
    }

    pub fn identity_seed(&self) -> Result<Option<Zeroizing<[u8; 32]>>, String> {
        self.read(IDENTITY_KEY_FILE)
            .map_err(|e| e.to_string())?
            .map(|bytes| decode_seed(&bytes))
            .transpose()
    }

    pub fn publish(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        bytes: &[u8],
    ) -> DirectoryResult<()> {
        if bytes.len() as u64 > MAX_BYTES {
            return Err(io::Error::other(
                "offline directory file exceeds byte limit",
            ));
        }
        let current = self.read(name)?;
        let slice = current.as_deref().map(Vec::as_slice);
        if slice == Some(bytes) {
            return Ok(());
        }
        if slice != expected {
            return Err(io::Error::other(CHANGED_ERR));
        }
        self.check()?;
        let temp = format!(".pending-{}", uuid::Uuid::new_v4());
        let mut file = openat(
            &self.file,
            temp.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | SAFE,
            0o600,
        )?;
        let res = (|| -> DirectoryResult<()> {
            disk(file.write_all(bytes))?;
            disk(file.sync_all())?;
            self.check()?;
            let rflags = expected.map_or(fs::RenameFlags::NOREPLACE, |_| fs::RenameFlags::empty());
            disk(fs::renameat_with(
                &self.file,
                temp.as_str(),
                &self.file,
                name,
                rflags,
            ))
        })();
        if res.is_err() {
            let _ = fs::unlinkat(&self.file, temp.as_str(), AtFlags::empty());
        }
        res?;
        disk(self.file.sync_all())?;
        (self.read(name)?.as_deref().map(Vec::as_slice) == Some(bytes))
            .then_some(())
            .ok_or_else(|| io::Error::other(CHANGED_ERR))
    }
}

fn stamp(m: &Metadata) -> (u64, u64, u64, u32, u32, i64, i64) {
    (
        m.dev(),
        m.ino(),
        m.len(),
        m.uid(),
        m.mode(),
        m.mtime(),
        m.mtime_nsec(),
    )
}
