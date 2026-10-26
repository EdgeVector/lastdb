//! File reachability outside immutable atom metadata.
//!
//! Blob counters count AtomCounter.blobs and live mk PerKeyRecord.meta refs.
//! Selected mgr bases also expose KeyMetadata through the normal generation
//! selector. History MutationEvent and conflict SyncConflict store atom IDs and
//! order, not independent KeyMetadata; the preceding atom proof covers them.
//! Unrelated tips rows over four MiB explicitly refuse this bounded source profile.
//! Unsupported metadata on a losing overlay also refuses before selection; no
//! stale reference is copied or silently treated as covered.
use super::{budget_json, expected, invalid, open_atom, read_directories, StorageResult};
use crate::atom::{atom_key_codec, file_pointer, KeyMetadata};
use crate::db_operations::atom_store::PerKeyRecord;
use crate::schema::types::field::build_storage_key;
use crate::storage::disk_pack::native_routing;
use cloud_backup_protocol::Digest;
use laststore::disk_pack::home::HomeRead;
use laststore::disk_pack::Current;
use laststore::disk_run::home::ReadBudget;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;

mod generation;
const ROW_MAX: u64 = 4 * 1024 * 1024;

/// Physical readers retain their own immutable root and release native pages
/// before these shared selectors decode metadata or an atom.
pub(crate) trait MetadataRead {
    fn get(&mut self, collection: &str, key: &str) -> StorageResult<Option<Vec<u8>>>;
    fn open_atom(&mut self, stored: &[u8]) -> StorageResult<crate::atom::Atom>;
    fn directories(
        &mut self,
        scope: Option<&str>,
        atom_id: &str,
        dependencies: &[super::FileDependency],
    ) -> StorageResult<()>;
}

/// Run after complete atom/directory/file-byte proof, against that same CURRENT.
/// Each native reader and metadata tree drops before the atom decoder starts.
pub fn verify_metadata_coverage(
    current: &Current,
    account_key: &[u8; 32],
    read: impl FnMut(ReadBudget) -> StorageResult<HomeRead>,
) -> StorageResult<u64> {
    let mut proof = MetadataProof::new(NativeMetadata {
        current,
        key: account_key,
        read,
    });
    let mut checked = 0u64;
    for group in 0..1024u16 {
        let mut start = Vec::new();
        loop {
            let page = {
                let mut read = proof.source.reader(32 * 1024 * 1024, 10000)?;
                let mut cursor = read
                    .administrative_group_range("tips", group, &start, &[0xff])
                    .map_err(super::super::map)?;
                cursor
                    .next_page_yielding_bounded(1, 0, 0, ROW_MAX)
                    .map_err(super::super::map)?
            };
            let next = page
                .resume
                .clone()
                .or_else(|| page.records.last().map(|row| row.key.as_bytes().to_vec()));
            for row in page.records {
                checked = checked
                    .checked_add(u64::from(proof.row(&row.key, row.body)?))
                    .ok_or_else(|| invalid("metadata proof count overflow"))?;
            }
            if page.exhausted {
                break;
            }
            let mut next = next.ok_or_else(|| invalid("metadata proof continuation"))?;
            if next.len() >= 65535 || (!start.is_empty() && next < start) {
                return Err(invalid("metadata proof continuation differs"));
            }
            next.push(0);
            start = next;
        }
    }
    Ok(checked)
}

struct NativeMetadata<'a, F> {
    current: &'a Current,
    key: &'a [u8; 32],
    read: F,
}
impl<F: FnMut(ReadBudget) -> StorageResult<HomeRead>> NativeMetadata<'_, F> {
    fn reader(&mut self, bytes: u64, keys: u64) -> StorageResult<HomeRead> {
        let read = (self.read)(ReadBudget {
            pages: 512,
            blocks: 8192,
            bytes,
            keys,
        })?;
        if read.current() != self.current {
            return Err(invalid("metadata proof root differs"));
        }
        Ok(read)
    }
}

impl<F: FnMut(ReadBudget) -> StorageResult<HomeRead>> MetadataRead for NativeMetadata<'_, F> {
    fn get(&mut self, collection: &str, key: &str) -> StorageResult<Option<Vec<u8>>> {
        self.reader(24 * 1024 * 1024, 1)?
            .get_bounded(collection, key, ROW_MAX)
            .map_err(super::super::map)
    }

    fn open_atom(&mut self, stored: &[u8]) -> StorageResult<crate::atom::Atom> {
        open_atom(self.key, stored, self.current, &mut self.read)
    }

    fn directories(
        &mut self,
        scope: Option<&str>,
        atom_id: &str,
        dependencies: &[super::FileDependency],
    ) -> StorageResult<()> {
        let mut read = self.reader(8 * 1024 * 1024, 3)?;
        read_directories(&mut read, scope, atom_id, dependencies).map(|_| ())
    }
}

/// The same live/generation selection and file-pointer coverage for either
/// physical format. The caller supplies complete, bounded metadata pages.
pub(crate) struct MetadataProof<R> {
    source: R,
}

impl<R: MetadataRead> MetadataProof<R> {
    pub(crate) fn new(source: R) -> Self {
        Self { source }
    }

    pub(crate) fn row(&mut self, key: &str, body: Vec<u8>) -> StorageResult<bool> {
        let raw = native_routing::decode(key)?;
        let raw = std::str::from_utf8(&raw).map_err(|_| invalid("metadata record key"))?;
        crate::storage::current_keys::validate("main", raw.as_bytes())?;
        let bare = crate::storage::current_keys::source_key(raw)?;
        let scope = crate::storage::current_keys::exact_scope(raw)?.map(str::to_owned);
        let record = if bare.starts_with("mk:") {
            self.live(scope.as_deref(), bare, body)?
        } else if bare.starts_with("mgr:v1:") {
            self.generation(scope.as_deref(), bare, body)?
        } else {
            None
        };
        let Some(record) = record else {
            return Ok(false);
        };
        let record = narrow(record)?;
        if refs(record.meta.as_ref())?.is_empty() {
            return Ok(false);
        }
        let atom_id = record.entry.atom_uuid.clone();
        Digest::try_from(atom_id.clone()).map_err(|_| invalid("metadata atom identity"))?;
        let metadata = record.meta.as_ref().and_then(|meta| meta.metadata.clone());
        drop(record);
        self.atom(scope.as_deref(), &atom_id, metadata.as_ref())?;
        Ok(true)
    }

    fn atom(
        &mut self,
        scope: Option<&str>,
        atom_id: &str,
        metadata: Option<&HashMap<String, String>>,
    ) -> StorageResult<()> {
        let raw = build_storage_key(scope, &atom_key_codec::body_key(atom_id));
        let body = self
            .source
            .get("atoms", &native_routing::encode(raw.as_bytes()))?
            .ok_or_else(|| invalid("metadata references an absent atom"))?;
        let atom = self.source.open_atom(&body)?;
        if atom.uuid() != atom_id {
            return Err(invalid("metadata atom identity differs"));
        }
        let dependencies = expected(&atom)?;
        // Only the authenticated exact atom can identify a duplicate private
        // account-v2 plaintext hash. Independent metadata refs still need proof.
        let refs = file_pointer::blob_refs_of_atom(atom.content(), metadata);
        if refs
            .iter()
            .any(|blob| !dependencies.iter().any(|item| item.blob_ref() == blob))
        {
            return Err(invalid(
                "live file metadata lacks certified atom pointer coverage",
            ));
        }
        drop(atom);
        drop(body);
        self.source.directories(scope, atom_id, &dependencies)?;
        Ok(())
    }

    fn get<T: DeserializeOwned>(
        &mut self,
        scope: Option<&str>,
        bare: &str,
    ) -> StorageResult<Option<T>> {
        let raw = build_storage_key(scope, bare);
        let row = self
            .source
            .get("tips", &native_routing::encode(raw.as_bytes()))?;
        row.map(|bytes| decode(bytes)).transpose()
    }
}

fn decode<T: DeserializeOwned>(bytes: Vec<u8>) -> StorageResult<T> {
    if bytes.len() as u64 > ROW_MAX {
        return Err(invalid("metadata record exceeds its bound"));
    }
    let value = budget_json::decode(&bytes)?;
    drop(bytes);
    serde_json::from_value(value).map_err(|_| invalid("metadata record format"))
}

fn refs(meta: Option<&KeyMetadata>) -> StorageResult<Vec<String>> {
    let Some(meta) = meta.filter(|meta| !meta.tombstoned) else {
        return Ok(Vec::new());
    };
    let refs = file_pointer::blob_refs_of_atom(&Value::Null, meta.metadata.as_ref());
    if refs.len() > 2 {
        return Err(invalid("metadata file reference count"));
    }
    for blob in &refs {
        let hash = blob
            .strip_prefix("sha256:")
            .ok_or_else(|| invalid("legacy metadata file identity"))?;
        Digest::try_from(hash.to_owned()).map_err(|_| invalid("metadata file identity"))?;
    }
    Ok(refs)
}

/// Preserve only small original selectors before another native read or decode.
/// Their names matter: file_hash can be a private digest, while file_blob_ref
/// always names storage. Classify them only against the authenticated atom.
fn narrow(mut record: PerKeyRecord) -> StorageResult<PerKeyRecord> {
    let selected = refs(record.meta.as_ref())?;
    entry_bound(&record.entry)?;
    if let Some(meta) = &mut record.meta {
        meta.source_file_name = None;
        meta.metadata = if selected.is_empty() {
            None
        } else {
            meta.metadata.take().map(|mut metadata| {
                ["file_blob_ref", "file_hash"]
                    .into_iter()
                    .filter_map(|name| metadata.remove(name).map(|value| (name.to_owned(), value)))
                    .collect()
            })
        };
    }
    Ok(record)
}

fn entry_bound(entry: &crate::atom::AtomEntry) -> StorageResult<()> {
    if serde_json::to_vec(entry)
        .map_err(|_| invalid("metadata tip identity"))?
        .len()
        > 16 * 1024
    {
        return Err(invalid("metadata tip identity exceeds release bound"));
    }
    Ok(())
}
