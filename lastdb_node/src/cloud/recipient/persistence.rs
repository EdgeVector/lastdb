use super::{CloudAccount, EnrollmentIntent, Path, PathBuf};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write as _;

pub(super) fn path(home: &Path, api_url: &str, account: &CloudAccount) -> Result<PathBuf, String> {
    let identity = serde_json::to_vec(&(api_url, account)).map_err(|error| error.to_string())?;
    let directory = home.join("cloud-recipients");
    if directory.exists()
        && !fs::symlink_metadata(&directory)
            .map_err(|error| error.to_string())?
            .file_type()
            .is_dir()
    {
        return Err("recipient directory must be a real directory".into());
    }
    Ok(directory.join(format!("{:x}.json", Sha256::digest(identity))))
}

pub(super) fn read(path: &Path) -> Result<Option<EnrollmentIntent>, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    if !metadata.file_type().is_file() || metadata.len() > 16 * 1024 {
        return Err("recipient enrollment must be a bounded regular file".into());
    }
    serde_json::from_slice(&fs::read(path).map_err(|error| error.to_string())?)
        .map(Some)
        .map_err(|_| "invalid saved recipient enrollment".into())
}

pub(super) fn write(path: &Path, intent: &EnrollmentIntent) -> Result<(), String> {
    let (temporary, parent) = temporary_file(path, intent)?;
    fs::rename(temporary, path).map_err(|error| error.to_string())?;
    sync_directory(&parent)
}

/// The first complete intent wins across processes. No cloud call starts
/// until the immutable enrollment UUID and its directory entry are durable.
pub(super) fn create(path: &Path, intent: &EnrollmentIntent) -> Result<bool, String> {
    let (temporary, parent) = temporary_file(path, intent)?;
    let created = match fs::hard_link(&temporary, path) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(error) => return Err(error.to_string()),
    };
    sync_directory(&parent)?;
    fs::remove_file(temporary).map_err(|error| error.to_string())?;
    sync_directory(&parent)?;
    Ok(created)
}

fn sync_directory(path: &Path) -> Result<(), String> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())
}

fn temporary_file(path: &Path, intent: &EnrollmentIntent) -> Result<(PathBuf, PathBuf), String> {
    use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
    let parent = path.parent().ok_or("recipient path lacks a parent")?;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(parent)
        .map_err(|error| error.to_string())?;
    if !fs::symlink_metadata(parent)
        .map_err(|error| error.to_string())?
        .file_type()
        .is_dir()
    {
        return Err("recipient directory must be a real directory".into());
    }
    let bytes = serde_json::to_vec(intent).map_err(|error| error.to_string())?;
    if bytes.len() > 16 * 1024 {
        return Err("recipient enrollment exceeds its size cap".into());
    }
    let temporary = parent.join(format!(".recipient-{}", uuid::Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| error.to_string())?;
    Ok((temporary, parent.to_path_buf()))
}
