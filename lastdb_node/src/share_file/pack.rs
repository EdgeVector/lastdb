use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use sha2::{Digest as _, Sha256};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

type RecordWrite<'a> = dyn FnMut(Vec<Value>) -> Result<String, String> + 'a;

/// Pack one file using bounded native writes and complete it by catalog CAS.
///
/// The caller holds exclusive app mutation access and the durable publication
/// hold from before this call until the catalog is complete again. A failure
/// retains its pending identity so recovery knows every possible chunk key.
pub fn add_file(
    input: &Path,
    schemas: &FileShareSchemas<'_>,
    catalog: Option<CatalogRecord<'_>>,
    mut write: impl FnMut(Vec<Value>) -> Result<String, String>,
) -> Result<FileManifest, String> {
    let mut catalog_state = Catalog::read(catalog.map(|record| record.entries))?;
    if catalog_state.pending.is_some() {
        return Err("recover the pending file share import before adding a file".into());
    }
    let revision = catalog.map(|record| record.revision);
    let name = input
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| valid_name(name))
        .ok_or("share file name must be valid UTF-8 of at most 255 bytes")?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(input)
        .map_err(|_| "cannot open share input file")?;
    let initial = file
        .metadata()
        .map_err(|_| "cannot inspect share input file")?;
    if !initial.is_file() || initial.len() > MAX_TOTAL_FILE_BYTES {
        return Err("share input must be a regular file within the file share size limit".into());
    }
    let id = Id::try_from(uuid::Uuid::new_v4().to_string())
        .map_err(|_| "cannot assign a share file identity")?;
    let mut manifest = FileManifest {
        id,
        name: name.to_owned(),
        size_bytes: initial.len(),
        chunks: chunk_count(initial.len()),
        sha256: digest_hex([0u8; 32])?,
    };
    // The placeholder reserves the exact final digest width before any write.
    catalog_state.files.push(manifest.clone());
    catalog_state.encode()?;
    catalog_state.files.pop();
    catalog_state.pending = Some(Pending {
        id: manifest.id.clone(),
        size_bytes: manifest.size_bytes,
    });
    let reservation = write(vec![catalog_state.mutation(schemas.catalog, revision)?])?;
    if reservation.is_empty() {
        return Err("file share reservation did not return a revision".into());
    }

    manifest.sha256 = pack_chunks(&file, schemas.chunks, &manifest, &mut write)?;
    if file_version(
        &file
            .metadata()
            .map_err(|_| "cannot inspect packed share file")?,
    ) != file_version(&initial)
    {
        return Err("share input file changed during import".into());
    }
    catalog_state.pending = None;
    catalog_state.files.push(manifest.clone());
    write(vec![
        catalog_state.mutation(schemas.catalog, Some(&reservation))?
    ])?;
    Ok(manifest)
}

fn pack_chunks(
    mut input: &File,
    schema: &str,
    manifest: &FileManifest,
    write: &mut RecordWrite<'_>,
) -> Result<Digest, String> {
    let mut buffer = vec![0u8; CHUNK_BYTES];
    let mut sha256 = Sha256::new();
    let mut batch = Vec::with_capacity(CHUNK_BATCH);
    for index in 0..manifest.chunks {
        let size = chunk_size(manifest.size_bytes, index);
        let bytes = &mut buffer[..size];
        input
            .read_exact(bytes)
            .map_err(|_| "cannot read complete share input file")?;
        sha256.update(&*bytes);
        batch.push(record_mutation(
            schema,
            chunk_key(&manifest.id, index),
            json!({CHUNK_DATA_FIELD: STANDARD.encode(bytes)}),
            Edit::Create,
        ));
        if batch.len() == CHUNK_BATCH {
            write(std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        write(batch)?;
    }
    digest_hex(sha256.finalize())
}

/// Remove exactly the possible chunks of an abandoned pending import, then
/// clear its reservation. Missing chunks are normal. The caller keeps the
/// durable publication hold and reloads the catalog after a successful cleanup.
pub fn recover_pending(
    schemas: &FileShareSchemas<'_>,
    catalog: CatalogRecord<'_>,
    mut read_revisions: impl FnMut(&[KeyValue]) -> Result<Vec<(KeyValue, String)>, String>,
    mut write: impl FnMut(Vec<Value>) -> Result<String, String>,
) -> Result<bool, String> {
    let mut state = Catalog::read(Some(catalog.entries))?;
    let Some(pending) = state.pending.take() else {
        return Ok(false);
    };
    let count = chunk_count(pending.size_bytes);
    for start in (0..count).step_by(CHUNK_BATCH) {
        let end = (start + CHUNK_BATCH as u64).min(count);
        let keys: Vec<_> = (start..end)
            .map(|index| chunk_key(&pending.id, index))
            .collect();
        let revisions = read_revisions(&keys)?;
        if revisions.len() > keys.len() {
            return Err("invalid share chunk recovery read".into());
        }
        let mut seen = HashSet::with_capacity(revisions.len());
        let mut purges = Vec::with_capacity(revisions.len());
        for (key, revision) in &revisions {
            if !keys.contains(key) || !seen.insert(key) || revision.is_empty() {
                return Err("invalid share chunk recovery read".into());
            }
            purges.push(record_mutation(
                schemas.chunks,
                key.clone(),
                json!({}),
                Edit::Purge(revision),
            ));
        }
        if !purges.is_empty() {
            write(purges)?;
        }
    }
    write(vec![
        state.mutation(schemas.catalog, Some(catalog.revision))?
    ])?;
    Ok(true)
}

fn file_version(metadata: &std::fs::Metadata) -> (u64, i64, i64, i64, i64) {
    (
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    )
}

fn digest_hex(digest: impl AsRef<[u8]>) -> Result<Digest, String> {
    Digest::try_from(fold_db::hex::hex_lower(digest))
        .map_err(|_| "invalid share file digest".into())
}
