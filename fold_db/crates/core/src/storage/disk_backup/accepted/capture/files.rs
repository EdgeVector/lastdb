//! External and inline file extraction and bounded `O_NOFOLLOW` file reads for capture.

use super::super::body;
use super::invalid;
use crate::schema::types::{FileWrite, LocalFileReference, Mutation};
use crate::storage::disk_pack;
use crate::storage::error::{StorageError, StorageResult};
use lastdb_model::hex::sha256_hex;
use lastdb_model::record_value::{FileMarker, InlineWrite};
use lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
use std::io::Read;
use std::path::{Path, PathBuf};

/// The external file of a write: what its body says, and the bytes to seal.
pub(super) struct External<'a> {
    pub(super) cleared: bool,
    pub(super) marker: Option<&'a FileMarker>,
    /// Hash of the sealed row in this store, for a file the write brought as a row.
    pub(super) sealed: Option<String>,
    pub(super) plain: Option<Vec<u8>>,
}

impl<'a> External<'a> {
    fn absent() -> Self {
        Self {
            cleared: false,
            marker: None,
            sealed: None,
            plain: None,
        }
    }

    fn clear() -> Self {
        Self {
            cleared: true,
            marker: None,
            sealed: None,
            plain: None,
        }
    }

    fn put(marker: &'a FileMarker, sealed: Option<String>, plain: Vec<u8>) -> Self {
        Self {
            cleared: false,
            marker: Some(marker),
            sealed,
            plain: Some(plain),
        }
    }
}

pub(super) async fn external_file<'a>(
    content_key: &[u8; 32],
    mutation: &'a Mutation,
    admit: &impl Fn(&LocalFileReference) -> Option<PathBuf>,
) -> StorageResult<External<'a>> {
    let Some(write) = mutation.file_write.as_ref() else {
        return Ok(External::absent());
    };
    match write {
        FileWrite::Clear if mutation.file.is_some() => {
            Err(invalid("accepted file clear still carries a marker"))
        }
        FileWrite::Clear => Ok(External::clear()),
        FileWrite::Put(_) => owned_row(content_key, mutation, write),
        FileWrite::LocalPath(_) => {
            let marker = file_marker(mutation, write)?;
            let plain = read_local(marker, write, admit).await?;
            Ok(External::put(marker, None, plain))
        }
    }
}

/// The bytes of a file that the write brought as a sealed row of this store.
fn owned_row<'a>(
    content_key: &[u8; 32],
    mutation: &'a Mutation,
    write: &'a FileWrite,
) -> StorageResult<External<'a>> {
    let marker = file_marker(mutation, write)?;
    let Some(row) = write.sealed_row() else {
        return Err(invalid("accepted file row is missing"));
    };
    let plain = crate::storage::format::records::decrypt_file_row(content_key, marker, row)
        .map_err(|_| invalid("accepted file row does not match its marker"))?;
    Ok(External::put(marker, Some(sha256_hex(row)), plain))
}

/// The bytes of a file that the write links by its path. They are read once,
/// here, so the entry can name the sealed file.
async fn read_local(
    marker: &FileMarker,
    write: &FileWrite,
    admit: &impl Fn(&LocalFileReference) -> Option<PathBuf>,
) -> StorageResult<Vec<u8>> {
    let reference = write
        .local_reference()
        .ok_or_else(|| invalid("accepted local file is missing"))?;
    let path = admit(reference)
        .ok_or_else(|| invalid("accepted local file was not issued for this database"))?;
    let size = marker.size_bytes;
    let hash = marker.sha256.clone();
    disk_pack::run_blocking(move || read_file(&path, size, &hash)).await
}

fn read_file(path: &Path, size: u64, hash: &str) -> StorageResult<Vec<u8>> {
    let unavailable = || invalid("accepted local file is not available");
    let differs = || invalid("accepted local file does not match its marker");
    if size > LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err(differs());
    }
    let file = disk_pack::admit::open_nofollow_read(path).map_err(|_| unavailable())?;
    let held = file.metadata().map_err(|_| unavailable())?;
    if !held.is_file() || held.len() != size {
        return Err(differs());
    }
    let cap = usize::try_from(size).map_err(|_| differs())?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(cap)
        .map_err(|_| StorageError::backend("accepted local file allocation refused"))?;
    file.take(size + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| unavailable())?;
    if bytes.len() as u64 == size && sha256_hex(&bytes) == hash {
        Ok(bytes)
    } else {
        Err(differs())
    }
}

fn file_marker<'a>(mutation: &'a Mutation, write: &'a FileWrite) -> StorageResult<&'a FileMarker> {
    let from_write = match write {
        FileWrite::Put(file) => file.marker(),
        FileWrite::LocalPath(file) => file.marker(),
        FileWrite::Clear => return Err(invalid("accepted file clear has no marker")),
    };
    if mutation
        .file
        .as_ref()
        .is_some_and(|marker| marker != from_write)
    {
        return Err(invalid("accepted file marker does not match the mutation"));
    }
    Ok(from_write)
}

pub(in crate::storage::disk_backup::accepted) fn inline_section(
    mutation: &Mutation,
) -> StorageResult<Option<body::InlineSection<'_>>> {
    match mutation.inline_write.as_ref() {
        None => Ok(None),
        Some(InlineWrite::Clear) => Ok(Some(body::inline_clear())),
        Some(InlineWrite::Put(file)) => Ok(Some(body::inline_put(file)?)),
    }
}
