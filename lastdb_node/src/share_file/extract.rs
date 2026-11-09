use super::*;
use crate::byte_cloud_restore::{StagedRecord, StagedRecordQuery};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// Read the file catalog from an already verified and held share snapshot.
pub fn list_files(
    schemas: &FileShareSchemas<'_>,
    mut read: impl FnMut(&StagedRecordQuery<'_>) -> Result<Option<StagedRecord>, String>,
) -> Result<Vec<FileManifest>, String> {
    let query = StagedRecordQuery::new(schemas.catalog, catalog_key(), false)
        .map_err(|_| "invalid file share catalog coordinates")?;
    let Some(record) = read(&query)? else {
        return Ok(Vec::new());
    };
    let catalog = Catalog::read(Some(string_field(&record, CATALOG_ENTRIES_FIELD)?))?;
    if catalog.pending.is_some() {
        return Err("file share snapshot contains an incomplete import".into());
    }
    Ok(catalog.files)
}

/// Verify and install a whole file at a new path. The callback reads each
/// exact key from the same held share snapshot for this entire call.
pub fn get_file(
    schemas: &FileShareSchemas<'_>,
    id: &Id,
    output: &Path,
    mut read: impl FnMut(&StagedRecordQuery<'_>) -> Result<Option<StagedRecord>, String>,
) -> Result<FileManifest, String> {
    let manifest = list_files(schemas, &mut read)?
        .into_iter()
        .find(|file| &file.id == id)
        .ok_or("file is absent from the share catalog")?;
    let pending = PendingOutput::new(output)?;
    let mut sha256 = Sha256::new();
    let mut writer = &pending.file;
    for index in 0..manifest.chunks {
        let query = StagedRecordQuery::new(schemas.chunks, chunk_key(id, index), false)
            .map_err(|_| "invalid share file chunk coordinates")?;
        let record = read(&query)?.ok_or("share file chunk is absent")?;
        let encoded = string_field(&record, CHUNK_DATA_FIELD)?;
        let size = chunk_size(manifest.size_bytes, index);
        if encoded.len() != size.div_ceil(3) * 4 {
            return Err("share file chunk differs from its expected size".into());
        }
        let bytes = STANDARD
            .decode(encoded)
            .map_err(|_| "invalid share file chunk encoding")?;
        if bytes.len() != size {
            return Err("share file chunk differs from its expected size".into());
        }
        sha256.update(&bytes);
        writer
            .write_all(&bytes)
            .map_err(|_| "cannot write share file output")?;
    }
    if fold_db::hex::hex_lower(sha256.finalize()) != manifest.sha256.as_str() {
        return Err("share file digest differs from its manifest".into());
    }
    pending.publish(output)?;
    Ok(manifest)
}

fn string_field<'a>(record: &'a StagedRecord, field: &str) -> Result<&'a str, String> {
    record
        .fields()
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| "file share record is missing its data field".into())
}

struct PendingOutput {
    path: PathBuf,
    file: File,
    directory: File,
    removed: bool,
}

impl PendingOutput {
    fn new(output: &Path) -> Result<Self, String> {
        let parent = output
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if output.file_name().is_none() {
            return Err("share file output needs a file name".into());
        }
        let directory = File::open(parent).map_err(|_| "cannot open share output directory")?;
        let path = parent.join(format!(
            ".lastdb-share-file-{}.pending",
            uuid::Uuid::new_v4()
        ));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .map_err(|_| "cannot create protected share file output")?;
        Ok(Self {
            path,
            file,
            directory,
            removed: false,
        })
    }

    fn publish(mut self, output: &Path) -> Result<(), String> {
        self.file
            .sync_all()
            .map_err(|_| "cannot sync share file output")?;
        fs::hard_link(&self.path, output).map_err(|_| "cannot install share file at a new path")?;
        fs::remove_file(&self.path).map_err(|_| "share file installed, temporary name remains")?;
        self.removed = true;
        self.directory
            .sync_all()
            .map_err(|_| "share file installed, cannot sync output directory")?;
        Ok(())
    }
}

impl Drop for PendingOutput {
    fn drop(&mut self) {
        if !self.removed {
            let _ = fs::remove_file(&self.path);
        }
    }
}
