use super::stream::RecordBody;
use crate::disk_pack::lifetime::GenerationHold;
use crate::disk_pack::tree_cursor::TreeCursor;
use crate::disk_pack::{
    artifact, corrupt, Current, DirectoryEntry, Location, MutationVersion, ReadCall, Source,
};
use crate::Result;
use std::io::Read;
use std::path::Path;

/// One administrative snapshot record, never a user molecule-query result.
pub struct StoredRecord<'a> {
    /// Exact collection identity, not a filesystem path.
    pub collection: &'a str,
    /// Encoded physical hash group, preserved during compatible conversion.
    pub group: u16,
    /// Complete encoded storage key.
    pub key: &'a str,
    /// Original batch and operation version.
    pub version: MutationVersion,
    /// Original opaque stored body size; zero for a tombstone or an empty body.
    pub body_bytes: u64,
    /// Stream to consume completely; None means a versioned tombstone.
    pub body: Option<&'a mut dyn Read>,
}

/// Visit an explicit held generation for backup conversion or reverse conversion.
/// It retains one cursor path and one record stream, with at most one million entries.
/// This API never becomes a product full-read query and never decrypts atom content.
pub fn visit_generation(
    root: &Path,
    current: &Current,
    visitor: impl FnMut(StoredRecord<'_>) -> Result<()>,
) -> Result<()> {
    visit(root, current, Some(1_000_000), visitor)
}

/// Stream a complete held generation with finite per-record cursor and I/O state.
/// Unlike the administrative call, this export has no logical record-count cap.
pub fn visit_generation_streaming(
    root: &Path,
    current: &Current,
    visitor: impl FnMut(StoredRecord<'_>) -> Result<()>,
) -> Result<()> {
    visit(root, current, None, visitor)
}

fn visit(
    root: &Path,
    current: &Current,
    maximum: Option<u64>,
    mut visitor: impl FnMut(StoredRecord<'_>) -> Result<()>,
) -> Result<()> {
    let _hold = GenerationHold::acquire(root, current)?;
    let mut call = selected_call(root, current, maximum, 0)?;
    let mut directory = TreeCursor::seek(&mut call, current.directory.clone(), b"")?;
    let mut count = 0u64;
    while let Some((key, value)) = directory.next(&mut call)? {
        let entry = DirectoryEntry::decode(&value)?;
        let collection = super::compact::collection(&key, entry.group)?;
        let directory_bytes = key.capacity() as u64 + collection.capacity() as u64;
        if maximum.is_none() {
            call = selected_call(
                root,
                current,
                maximum,
                directory.retained_bytes()? + directory_bytes,
            )?;
        }
        let mut cursor = TreeCursor::seek(&mut call, entry.locator.clone(), b"")?;
        while let Some((key_bytes, value)) = cursor.next(&mut call)? {
            count = count
                .checked_add(1)
                .ok_or_else(crate::disk_pack::resource)?;
            if maximum.is_some_and(|maximum| count > maximum) {
                return Err(crate::disk_pack::resource());
            }
            let location = Location::decode_for_current(&value, current)?;
            if location.version.batch > current.frontier {
                return Err(corrupt("visit record beyond selected frontier"));
            }
            let retained =
                directory.retained_bytes()? + directory_bytes + cursor.retained_bytes()?;
            let active = retained + key_bytes.capacity() as u64;
            let key =
                std::str::from_utf8(&key_bytes).map_err(|_| corrupt("visit key is not UTF-8"))?;
            let version = location.version;
            if location.source == Source::Tombstone {
                visitor(StoredRecord {
                    collection: &collection,
                    group: entry.group,
                    key,
                    version,
                    body_bytes: 0,
                    body: None,
                })?;
            } else {
                let mut body = RecordBody::open_with_retained(
                    &mut call,
                    &collection,
                    key,
                    location,
                    maximum.is_none().then_some(active),
                )?;
                visitor(StoredRecord {
                    collection: &collection,
                    group: entry.group,
                    key,
                    version,
                    body_bytes: body.body_bytes(),
                    body: Some(&mut body),
                })?;
                if body.body_bytes() != 0 {
                    return Err(corrupt(
                        "administrative visitor did not consume complete record",
                    ));
                }
            }
            drop(key_bytes);
            call = selected_call(root, current, maximum, retained)?;
        }
        drop(cursor);
        drop(collection);
        drop(key);
        if maximum.is_none() {
            call = selected_call(root, current, maximum, directory.retained_bytes()?)?;
        }
    }
    Ok(())
}

fn selected_call(
    root: &Path,
    current: &Current,
    maximum: Option<u64>,
    retained: u64,
) -> Result<ReadCall> {
    let mut call = artifact::metadata_call(root, current.clone());
    if maximum.is_none() {
        call.budget.bytes = lastdb_storage_policy::BACKUP_RESOURCES.decoded_values_bytes;
        call.charge(retained)?;
    }
    Ok(call)
}
