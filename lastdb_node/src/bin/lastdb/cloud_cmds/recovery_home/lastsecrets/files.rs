//! OS-account tool/config binding. Read metadata, never a stored secret value.

use std::ffi::CStr;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};

pub(super) struct ProtectedFile {
    pub(super) path: PathBuf,
    pub(super) descriptor: File,
    pub(super) bytes: Vec<u8>,
    pub(super) sha256: String,
    device: u64,
    inode: u64,
    limit: u64,
    executable: bool,
}

impl ProtectedFile {
    pub(super) fn executable(path: &Path, home: &Path) -> Result<Self, &'static str> {
        secure_ancestors(path.parent().ok_or("invalid_lastsecrets_executable_path")?)?;
        let canonical = std::fs::canonicalize(path).map_err(|_| "trusted_lastsecrets_missing")?;
        if !canonical.starts_with(home) {
            return Err("untrusted_lastsecrets_executable_path");
        }
        Self::open(&canonical, true, 128 * 1024 * 1024)
    }

    pub(super) fn config(path: &Path) -> Result<Self, &'static str> {
        Self::open(path, false, 65_536)
    }

    fn open(path: &Path, executable: bool, limit: u64) -> Result<Self, &'static str> {
        secure_ancestors(path.parent().ok_or("invalid_lastsecrets_file_path")?)?;
        let mut descriptor = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .map_err(|_| "cannot_open_trusted_lastsecrets_file")?;
        let metadata = descriptor
            .metadata()
            .map_err(|_| "cannot_inspect_lastsecrets_file")?;
        validate_file(&metadata, executable, limit)?;
        let sha256 = digest(&mut descriptor, limit)?;
        let mut bytes = Vec::new();
        if !executable {
            descriptor
                .rewind()
                .map_err(|_| "cannot_read_lastsecrets_config")?;
            (&mut descriptor)
                .take(limit + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| "cannot_read_lastsecrets_config")?;
            if bytes.len() as u64 > limit {
                return Err("lastsecrets_config_exceeds_supported_size");
            }
        }
        Ok(Self {
            path: path.to_owned(),
            descriptor,
            bytes,
            sha256,
            device: metadata.dev(),
            inode: metadata.ino(),
            limit,
            executable,
        })
    }

    pub(super) fn verify(&self) -> Result<(), &'static str> {
        let current = Self::open(&self.path, self.executable, self.limit)?;
        if current.device != self.device
            || current.inode != self.inode
            || current.sha256 != self.sha256
        {
            return Err("trusted_lastsecrets_file_changed");
        }
        let retained = self
            .descriptor
            .metadata()
            .map_err(|_| "cannot_inspect_retained_lastsecrets_file")?;
        validate_file(&retained, self.executable, self.limit)?;
        if retained.dev() != current.device || retained.ino() != current.inode {
            return Err("trusted_lastsecrets_file_changed");
        }
        Ok(())
    }
}

pub(super) fn account_home() -> Result<PathBuf, &'static str> {
    let mut account = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut buffer = vec![0_u8; 65_536];
    let mut result = std::ptr::null_mut();
    let owner = unsafe { libc::geteuid() };
    let rc = unsafe {
        libc::getpwuid_r(
            owner,
            account.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if rc != 0 || result.is_null() {
        return Err("cannot_resolve_os_account_home");
    }
    let account = unsafe { account.assume_init() };
    if account.pw_uid != owner || account.pw_dir.is_null() {
        return Err("invalid_os_account_home");
    }
    let home = PathBuf::from(std::ffi::OsStr::from_bytes(
        unsafe { CStr::from_ptr(account.pw_dir) }.to_bytes(),
    ));
    secure_ancestors(&home)?;
    let metadata =
        std::fs::symlink_metadata(&home).map_err(|_| "cannot_inspect_os_account_home")?;
    if metadata.uid() != owner {
        return Err("invalid_os_account_home_owner");
    }
    Ok(home)
}

pub(super) fn require_no_runtime_config(home: &Path) -> Result<(), &'static str> {
    // Bun standalone programs can autoload these files. A fixed root cwd and
    // explicit refusal prevent .env/preload config from restoring ambient routes.
    for path in [
        PathBuf::from("/.env"),
        PathBuf::from("/.env.local"),
        PathBuf::from("/.env.production"),
        PathBuf::from("/.env.production.local"),
        PathBuf::from("/.env.development"),
        PathBuf::from("/.env.development.local"),
        PathBuf::from("/bunfig.toml"),
        home.join(".bunfig.toml"),
        home.join(".config/.bunfig.toml"),
    ] {
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err("lastsecrets_automatic_runtime_config_present"),
        }
    }
    Ok(())
}

pub(super) fn require_owner_socket(socket: &Path) -> Result<(), &'static str> {
    secure_ancestors(socket.parent().ok_or("invalid_lastsecrets_socket_path")?)?;
    let metadata = std::fs::symlink_metadata(socket)
        .map_err(|_| "lastsecrets_default_owner_socket_missing")?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o777 != 0o600
    {
        return Err("invalid_lastsecrets_default_owner_socket");
    }
    Ok(())
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Config {
    config_version: u64,
    node_url: String,
    node_socket_path: PathBuf,
    user_hash: String,
    schema_hash: String,
    schema_name: String,
    index_schema_hash: String,
    index_schema_name: String,
}

pub(super) fn require_private_config(bytes: &[u8], socket: &Path) -> Result<(), &'static str> {
    let config: Config =
        serde_json::from_slice(bytes).map_err(|_| "invalid_trusted_lastsecrets_config")?;
    if config.config_version != 1
        || config.node_socket_path != socket
        || !matches!(
            config.node_url.as_str(),
            "http://localhost"
                | "http://127.0.0.1"
                | "http://localhost:9001"
                | "http://127.0.0.1:9001"
        )
        || config.schema_name != "lastsecrets/LastSecret"
        || config.index_schema_name != "lastsecrets/LastSecretIndex"
        || config.user_hash.is_empty()
        || config.schema_hash.is_empty()
        || config.index_schema_hash.is_empty()
    {
        return Err("lastsecrets_config_is_not_preserved_default_local_route");
    }
    Ok(())
}

fn secure_ancestors(path: &Path) -> Result<(), &'static str> {
    if !path.is_absolute() {
        return Err("lastsecrets_path_must_be_absolute");
    }
    let mut current = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => current.push(name),
            _ => return Err("invalid_lastsecrets_path_component"),
        }
        let metadata =
            std::fs::symlink_metadata(&current).map_err(|_| "cannot_inspect_lastsecrets_path")?;
        if !metadata.is_dir()
            || (metadata.uid() != 0 && metadata.uid() != unsafe { libc::geteuid() })
            || metadata.mode() & 0o022 != 0
        {
            return Err("lastsecrets_path_is_not_owner_protected");
        }
    }
    Ok(())
}

fn validate_file(
    metadata: &std::fs::Metadata,
    executable: bool,
    limit: u64,
) -> Result<(), &'static str> {
    let permissions = metadata.mode() & 0o777;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.nlink() != 1
        || metadata.len() == 0
        || metadata.len() > limit
        || permissions & 0o022 != 0
        || (executable && permissions & 0o111 == 0)
        || (!executable && permissions != 0o600)
    {
        return Err("invalid_trusted_lastsecrets_file_metadata");
    }
    Ok(())
}

fn digest(file: &mut File, limit: u64) -> Result<String, &'static str> {
    file.rewind()
        .map_err(|_| "cannot_read_trusted_lastsecrets_file")?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 16_384];
    let mut total = 0;
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| "cannot_read_trusted_lastsecrets_file")?;
        if count == 0 {
            break;
        }
        total += count as u64;
        if total > limit {
            return Err("lastsecrets_file_exceeds_supported_size");
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex::encode(hasher.finalize()))
}
