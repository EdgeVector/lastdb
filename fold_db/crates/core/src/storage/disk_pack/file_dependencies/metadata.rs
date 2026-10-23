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
use crate::storage::laststore::native_routing;
use cloud_backup_protocol::Digest;
use laststore::disk_pack::home::HomeRead;
use laststore::disk_pack::{Current, RangeRecord, ReadBudget};
use serde::de::DeserializeOwned;
use serde_json::Value;

mod generation;
const ROW_MAX: u64 = 4 * 1024 * 1024;

/// Run after complete atom/directory/file-byte proof, against that same CURRENT.
/// Each native reader and metadata tree drops before the atom decoder starts.
pub fn verify_metadata_coverage(
    current: &Current,
    account_key: &[u8; 32],
    read: impl FnMut(ReadBudget) -> StorageResult<HomeRead>,
) -> StorageResult<u64> {
    let mut proof = Proof {
        current,
        key: account_key,
        read,
    };
    let mut checked = 0u64;
    for group in 0..1024u16 {
        let mut start = Vec::new();
        loop {
            let page = {
                let mut read = proof.reader(32 * 1024 * 1024, 10000)?;
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
                    .checked_add(u64::from(proof.row(row)?))
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

struct Proof<'a, F> {
    current: &'a Current,
    key: &'a [u8; 32],
    read: F,
}
impl<F: FnMut(ReadBudget) -> StorageResult<HomeRead>> Proof<'_, F> {
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

    fn row(&mut self, row: RangeRecord) -> StorageResult<bool> {
        let raw = native_routing::decode(&row.key)?;
        let raw = std::str::from_utf8(&raw).map_err(|_| invalid("metadata record key"))?;
        crate::storage::current_keys::validate("main", raw.as_bytes())?;
        let bare = crate::storage::current_keys::source_key(raw)?;
        let scope = crate::storage::current_keys::exact_scope(raw)?.map(str::to_owned);
        let record = if bare.starts_with("mk:") {
            self.live(scope.as_deref(), bare, row.body)?
        } else if bare.starts_with("mgr:v1:") {
            self.generation(scope.as_deref(), bare, row.body)?
        } else {
            None
        };
        let Some(record) = record else {
            return Ok(false);
        };
        let record = narrow(record)?;
        let refs = refs(record.meta.as_ref())?;
        if refs.is_empty() {
            return Ok(false);
        }
        let atom_id = record.entry.atom_uuid.clone();
        Digest::try_from(atom_id.clone()).map_err(|_| invalid("metadata atom identity"))?;
        drop(record);
        self.atom(scope.as_deref(), &atom_id, &refs)?;
        Ok(true)
    }

    fn atom(&mut self, scope: Option<&str>, atom_id: &str, refs: &[String]) -> StorageResult<()> {
        let raw = build_storage_key(scope, &atom_key_codec::body_key(atom_id));
        let body = {
            let mut read = self.reader(24 * 1024 * 1024, 1)?;
            read.get_bounded("atoms", &native_routing::encode(raw.as_bytes()), ROW_MAX)
                .map_err(super::super::map)?
                .ok_or_else(|| invalid("metadata references an absent atom"))?
        };
        let atom = open_atom(self.key, &body)?;
        if atom.uuid() != atom_id {
            return Err(invalid("metadata atom identity differs"));
        }
        let dependencies = expected(&atom)?;
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
        let mut read = self.reader(8 * 1024 * 1024, 3)?;
        read_directories(&mut read, scope, atom_id, &dependencies, self.key)?;
        Ok(())
    }

    fn get<T: DeserializeOwned>(
        &mut self,
        scope: Option<&str>,
        bare: &str,
    ) -> StorageResult<Option<T>> {
        let raw = build_storage_key(scope, bare);
        let row = {
            let mut read = self.reader(24 * 1024 * 1024, 1)?;
            read.get_bounded("tips", &native_routing::encode(raw.as_bytes()), ROW_MAX)
                .map_err(super::super::map)?
        };
        row.map(|bytes| decode(self.key, bytes)).transpose()
    }
}

fn decode<T: DeserializeOwned>(key: &[u8; 32], bytes: Vec<u8>) -> StorageResult<T> {
    let plain = crate::crypto::at_rest::open_at_rest_bounded_owned(key, bytes, ROW_MAX, ROW_MAX)
        .map_err(|_| invalid("metadata record envelope"))?;
    let value = budget_json::decode(&plain)?;
    drop(plain);
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

/// Preserve only small selectors before another native read or atom decode.
fn narrow(mut record: PerKeyRecord) -> StorageResult<PerKeyRecord> {
    let selected = refs(record.meta.as_ref())?;
    entry_bound(&record.entry)?;
    if let Some(meta) = &mut record.meta {
        meta.source_file_name = None;
        meta.metadata = match selected.as_slice() {
            [] => None,
            [first] => Some(std::collections::HashMap::from([(
                "file_blob_ref".into(),
                first.clone(),
            )])),
            [first, second] => Some(std::collections::HashMap::from([
                ("file_blob_ref".into(), first.clone()),
                ("file_hash".into(), second[7..].to_owned()),
            ])),
            _ => return Err(invalid("metadata file reference count")),
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
