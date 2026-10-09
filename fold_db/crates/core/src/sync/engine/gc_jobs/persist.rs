//! Themed module split from the parent.

use super::*;

pub(super) fn io_error(error: impl std::fmt::Display) -> SyncError {
    SyncError::Storage(format!("GC receipt persistence: {error}"))
}

pub(super) fn index_version_default() -> u32 {
    GC_RECEIPT_MIN_VERSION
}

/// Accept receipt versions 1 and 2. Refuse anything else by name.
pub fn check_receipt_version(version: Option<u64>) -> SyncResult<()> {
    match version {
        Some(v) if (u64::from(GC_RECEIPT_MIN_VERSION)..=u64::from(GC_RECEIPT_VERSION)).contains(&v) => {
            Ok(())
        }
        Some(v) => Err(io_error(format!(
            "{GC_RECEIPT_VERSION_UNSUPPORTED}: receipt version {v}; this reader accepts {GC_RECEIPT_MIN_VERSION} to {GC_RECEIPT_VERSION}; no recovery or dispatch"
        ))),
        None => Err(io_error(format!(
            "{GC_RECEIPT_VERSION_UNSUPPORTED}: receipt version missing; no recovery or dispatch"
        ))),
    }
}

pub(super) fn read<T: DeserializeOwned>(path: &Path) -> SyncResult<T> {
    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).map_err(io_error)?).map_err(io_error)?;
    check_receipt_version(value.get("version").and_then(serde_json::Value::as_u64))?;
    serde_json::from_value(value).map_err(io_error)
}

/// Stamp the format version from the fields present, then persist.
pub(super) fn put_job(path: &Path, job: &mut GcJobReceipt) -> SyncResult<()> {
    job.version = job.format_version();
    put(path, job)
}

pub(super) fn put_object(path: &Path, object: &mut GcObjectReceipt) -> SyncResult<()> {
    object.version = object.format_version();
    put(path, object)
}

pub(super) fn put(path: &Path, value: &impl Serialize) -> SyncResult<()> {
    put_file(path, value)?;
    sync_parent(path)
}

pub(super) fn put_file(path: &Path, value: &impl Serialize) -> SyncResult<()> {
    let parent = path.parent().ok_or_else(|| io_error("missing parent"))?;
    std::fs::create_dir_all(parent).map_err(io_error)?;
    let mut file = tempfile::NamedTempFile::new_in(parent).map_err(io_error)?;
    serde_json::to_writer(&mut file, value).map_err(io_error)?;
    file.flush().map_err(io_error)?;
    file.as_file().sync_all().map_err(io_error)?;
    file.persist(path).map_err(io_error)?;
    Ok(())
}

pub(super) fn sync_parent(path: &Path) -> SyncResult<()> {
    let parent = path.parent().ok_or_else(|| io_error("missing parent"))?;
    std::fs::File::open(parent)
        .and_then(|dir| dir.sync_all())
        .map_err(io_error)
}
