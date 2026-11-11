//! Descriptor-relative, no-symlink credentials and exclusive destination creation.

use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path};

use rustix::fs::{self, AtFlags, Mode, OFlags};
use zeroize::Zeroizing;

fn directory_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

pub(crate) fn open_directory(path: &Path) -> Result<File, String> {
    open_directory_excluding(path, None)
}

fn open_directory_excluding(path: &Path, excluded: Option<&File>) -> Result<File, String> {
    require_absolute(path)?;
    let mut directory = File::from(
        fs::open("/", directory_flags(), Mode::empty())
            .map_err(|_| "cannot open directory root")?,
    );
    require_distinct_directory(&directory, excluded)?;
    for component in path.components() {
        if let Component::Normal(name) = component {
            directory = File::from(
                fs::openat(&directory, name, directory_flags(), Mode::empty())
                    .map_err(|_| "directory path must exist without symlink components")?,
            );
            require_distinct_directory(&directory, excluded)?;
        }
    }
    Ok(directory)
}

fn require_distinct_directory(directory: &File, excluded: Option<&File>) -> Result<(), String> {
    let Some(excluded) = excluded else {
        return Ok(());
    };
    let current = fs::fstat(directory).map_err(|_| "cannot inspect destination ancestor")?;
    let source = fs::fstat(excluded).map_err(|_| "cannot inspect source directory")?;
    if current.st_dev == source.st_dev && current.st_ino == source.st_ino {
        return Err("destination parent must not contain the source home or its aliases".into());
    }
    Ok(())
}

fn require_absolute(path: &Path) -> Result<(), String> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
    {
        return Err("credential-home paths must be absolute without parent components".into());
    }
    Ok(())
}

pub(super) fn require_disjoint(source: &Path, destination: &Path) -> Result<(), String> {
    require_absolute(source)?;
    require_absolute(destination)?;
    if source.starts_with(destination) || destination.starts_with(source) {
        return Err("source and recovery home paths must not overlap".into());
    }
    Ok(())
}

pub(crate) fn require_owned_directory(directory: &File) -> Result<(), String> {
    let metadata = directory
        .metadata()
        .map_err(|_| "cannot inspect source home")?;
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.permissions().mode() & 0o022 != 0 {
        return Err("source home must be owner-held and not writable by other users".into());
    }
    Ok(())
}

pub(crate) fn require_mode_0700_directory(
    directory: &File,
    error: &'static str,
) -> Result<(), String> {
    let meta = directory.metadata().map_err(|_| error)?;
    if meta.uid() != unsafe { libc::geteuid() } || meta.permissions().mode() & 0o777 != 0o700 {
        return Err(error.to_owned());
    }
    Ok(())
}

pub(crate) fn require_private_file(
    file: &File,
    limit: Option<u64>,
    error: &'static str,
) -> Result<(), String> {
    let meta = file.metadata().map_err(|_| error)?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.permissions().mode() & 0o777 != 0o600
        || meta.nlink() != 1
        || limit.is_some_and(|max| meta.len() > max)
    {
        return Err(error.to_owned());
    }
    Ok(())
}

pub(crate) fn entry_present(directory: &File, name: &str) -> Result<bool, String> {
    match fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => Ok(true),
        Err(rustix::io::Errno::NOENT) => Ok(false),
        Err(_) => Err("cannot inspect credential-home entry".into()),
    }
}

pub(crate) fn read_private_file(
    directory: &File,
    name: &str,
    limit: u64,
) -> Result<Vec<u8>, String> {
    read_private_file_bound(directory, name, limit, None)
}

pub(crate) fn read_private_file_bound(
    directory: &File,
    name: &str,
    limit: u64,
    expected: Option<&File>,
) -> Result<Vec<u8>, String> {
    let file = File::from(
        fs::openat(
            directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|_| "cannot open protected credential file")?,
    );
    if let Some(expected) = expected {
        let actual = fs::fstat(&file).map_err(|_| "cannot inspect credential read-back")?;
        let held = fs::fstat(expected).map_err(|_| "cannot inspect held credential file")?;
        if actual.st_dev != held.st_dev || actual.st_ino != held.st_ino {
            return Err("recovery credential file changed during preparation".into());
        }
    }
    require_private_file(
        &file,
        Some(limit),
        "credential file must be a bounded owner-only regular file",
    )?;
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read protected credential file")?;
    if bytes.len() as u64 > limit {
        return Err("credential file exceeds the supported size".into());
    }
    Ok(bytes)
}

pub(crate) struct FreshHome {
    parent: File,
    name: OsString,
    directory: Option<File>,
    created_files: Vec<CreatedFile>,
    committed: bool,
}

struct CreatedFile {
    name: &'static str,
    descriptor: File,
    expected: Zeroizing<Vec<u8>>,
}

impl FreshHome {
    pub(crate) fn preflight_empty(destination: &Path) -> Result<Self, String> {
        Self::preflight_with_exclusion(destination, None)
    }

    pub(super) fn preflight_empty_parent(destination: &Path) -> Result<Self, String> {
        let target = Self::preflight_empty(destination)?;
        require_mode_0700_directory(
            &target.parent,
            "credential parent must be an owner-only directory",
        )?;
        for entry in
            fs::Dir::read_from(&target.parent).map_err(|_| "cannot read credential parent")?
        {
            let entry = entry.map_err(|_| "cannot inspect credential parent")?;
            if !matches!(entry.file_name().to_bytes(), b"." | b"..") {
                return Err("credential parent must be empty".into());
            }
        }
        Ok(target)
    }

    pub(super) fn preflight(destination: &Path, source: &File) -> Result<Self, String> {
        Self::preflight_with_exclusion(destination, Some(source))
    }

    /// The online DEV action cannot create any child below the primary home.
    /// Use a fixed temporary namespace; no HOME or TMPDIR override is accepted.
    pub(crate) fn preflight_source_free_dev(destination: &Path) -> Result<Self, String> {
        #[cfg(target_os = "macos")]
        let temporary_root = Path::new("/private/tmp");
        #[cfg(not(target_os = "macos"))]
        let temporary_root = Path::new("/tmp");
        let parent = destination.parent().ok_or("missing DEV recovery parent")?;
        if parent.parent() != Some(temporary_root)
            || !parent
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("lastdb-dev-recovery."))
        {
            return Err("DEV recovery requires a fresh private temporary parent".into());
        }
        Self::preflight_empty_parent(destination)
    }

    fn preflight_with_exclusion(destination: &Path, source: Option<&File>) -> Result<Self, String> {
        require_absolute(destination)?;
        let name = destination
            .file_name()
            .ok_or("destination requires a home name")?;
        let parent = open_directory_excluding(
            destination.parent().ok_or("missing destination parent")?,
            source,
        )?;
        let metadata = parent
            .metadata()
            .map_err(|_| "cannot inspect destination parent")?;
        if metadata.permissions().mode() & 0o022 != 0 && metadata.permissions().mode() & 0o1000 == 0
        {
            return Err(
                "destination parent must prevent other users from replacing its entries".into(),
            );
        }
        if !matches!(
            fs::statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW),
            Err(rustix::io::Errno::NOENT)
        ) {
            return Err("recovery home must not already exist".into());
        }
        Ok(Self {
            parent,
            name: name.to_os_string(),
            directory: None,
            created_files: Vec::new(),
            committed: false,
        })
    }

    pub(crate) fn create(mut self) -> Result<Self, String> {
        fs::mkdirat(&self.parent, &self.name, Mode::from_raw_mode(0o700))
            .map_err(|_| "cannot exclusively create recovery home")?;
        let directory = fs::openat(&self.parent, &self.name, directory_flags(), Mode::empty())
            .map_err(|_| "cannot open new recovery home without symlinks")?;
        self.directory = Some(directory.into());
        fs::fchmod(self.directory(), Mode::from_raw_mode(0o700))
            .map_err(|_| "cannot protect recovery home")?;
        require_owned_directory(self.directory())?;
        Ok(self)
    }

    fn directory(&self) -> &File {
        self.directory.as_ref().expect("created recovery home")
    }

    pub(crate) fn write(&mut self, name: &'static str, bytes: &[u8]) -> Result<(), String> {
        let descriptor = fs::openat(
            self.directory(),
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
        )
        .map_err(|_| "cannot exclusively create recovery credential file")?;
        let mut file = File::from(descriptor);
        self.created_files.push(CreatedFile {
            name,
            descriptor: file
                .try_clone()
                .map_err(|_| "cannot retain new credential file")?,
            expected: Zeroizing::new(bytes.to_vec()),
        });
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| "cannot protect recovery credential file")?;
        file.write_all(bytes)
            .map_err(|_| "cannot write recovery credential file")?;
        file.sync_all()
            .map_err(|_| "cannot sync recovery credential file")?;
        let observed = Zeroizing::new(read_private_file(
            self.directory(),
            name,
            bytes.len() as u64,
        )?);
        if observed.as_slice() != bytes {
            return Err("recovery credential read-back does not match".into());
        }
        Ok(())
    }

    pub(crate) fn commit(&mut self) -> Result<(), String> {
        if !self.owns_destination() {
            return Err("recovery home changed during preparation".into());
        }
        self.verify_final_files()?;
        self.directory()
            .sync_all()
            .map_err(|_| "cannot sync recovery home")?;
        self.parent
            .sync_all()
            .map_err(|_| "cannot sync recovery home parent")?;
        self.committed = true;
        Ok(())
    }

    fn verify_final_files(&self) -> Result<(), String> {
        require_mode_0700_directory(
            self.directory(),
            "recovery home requires the exact protected credential files",
        )?;
        if self.created_files.is_empty() {
            return Err("recovery home requires the exact protected credential files".into());
        }
        let entries = fs::Dir::read_from(self.directory())
            .map_err(|_| "cannot inspect final recovery-home entries")?;
        let mut count = 0;
        for entry in entries {
            let entry = entry.map_err(|_| "cannot inspect final recovery-home entry")?;
            let name = entry.file_name().to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            if !self
                .created_files
                .iter()
                .any(|file| file.name.as_bytes() == name)
            {
                return Err("recovery home contains an unexpected entry".into());
            }
            count += 1;
        }
        if count != self.created_files.len() {
            return Err("recovery home protected credential count differs".into());
        }
        for file in &self.created_files {
            let bytes = Zeroizing::new(read_private_file_bound(
                self.directory(),
                file.name,
                file.expected.len() as u64,
                Some(&file.descriptor),
            )?);
            if bytes.as_slice() != file.expected.as_slice() {
                return Err("final recovery credential contents do not match".into());
            }
        }
        Ok(())
    }

    fn owns_destination(&self) -> bool {
        same_inode(&self.parent, &self.name, self.directory())
    }
}

pub(crate) fn same_inode<P: rustix::path::Arg>(directory: &File, name: P, held: &File) -> bool {
    fs::fstat(held).is_ok_and(|held| {
        fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
            .is_ok_and(|entry| entry.st_dev == held.st_dev && entry.st_ino == held.st_ino)
    })
}

impl Drop for FreshHome {
    fn drop(&mut self) {
        if self.committed || self.directory.is_none() {
            return;
        }
        for file in &self.created_files {
            if same_inode(self.directory(), file.name, &file.descriptor) {
                let _ = fs::unlinkat(self.directory(), file.name, AtFlags::empty());
            }
        }
        if self.owns_destination() {
            let _ = fs::unlinkat(&self.parent, &self.name, AtFlags::REMOVEDIR);
        }
    }
}
