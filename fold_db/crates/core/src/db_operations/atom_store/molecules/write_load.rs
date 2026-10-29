//! One keyed write fetches its changed fields through shared bounded batches.

use crate::atom::{molecule_key_codec, AtomEntry, KeyMetadata, MoleculeHashRange};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use std::collections::{BTreeMap, HashSet};

use super::super::{AtomStore, ChangedKey, MoleculeData, MoleculeHeader};

const READ_BATCH_KEYS: usize = 10_000;
type Record = (String, String, AtomEntry, Option<KeyMetadata>);

pub(crate) struct MoleculeWriteLoad<'a> {
    pub(crate) molecule: &'a str,
    pub(crate) scope: Option<&'a str>,
    pub(crate) changed: &'a HashSet<ChangedKey>,
}

struct Slot {
    request: usize,
    hash: String,
    range: String,
    key: String,
}

impl AtomStore {
    /// Every header is exact-scope. Missing headers retain the caller's full
    /// load fallback. Existing selectors resolve tips, generation shadows,
    /// private changes and Delete barriers at the same captured write view.
    pub(crate) async fn load_molecules_for_write(
        &self,
        requests: &[MoleculeWriteLoad<'_>],
    ) -> Result<Vec<Option<MoleculeData>>, SchemaError> {
        let keys: Vec<_> = requests
            .iter()
            .map(|request| {
                build_storage_key(
                    request.scope,
                    &molecule_key_codec::header_key(request.molecule),
                )
            })
            .collect();
        let mut headers = Vec::with_capacity(keys.len());
        for keys in keys.chunks(READ_BATCH_KEYS) {
            headers.extend(self.main_store.get_items::<MoleculeHeader>(keys).await?);
        }
        if headers.len() != requests.len() {
            return Err(alignment_error());
        }
        let mut by_scope: BTreeMap<Option<&str>, Vec<Slot>> = BTreeMap::new();
        for (position, request) in requests.iter().enumerate() {
            if headers[position].is_none() {
                continue;
            }
            for changed in request.changed {
                let (hash, range) = (changed.hash_or_empty(), changed.range_or_empty());
                let key = molecule_key_codec::hash_range_record_key(
                    request.molecule,
                    &self.storage_hash(request.molecule, hash)?,
                    &self.storage_range(request.molecule, range)?,
                );
                by_scope.entry(request.scope).or_default().push(Slot {
                    request: position,
                    hash: hash.to_owned(),
                    range: range.to_owned(),
                    key,
                });
            }
        }
        let selected = futures::future::try_join_all(
            by_scope
                .into_iter()
                .map(|(scope, slots)| self.load_write_scope(scope, slots)),
        )
        .await?;
        let mut records: Vec<Vec<Record>> = vec![Vec::new(); requests.len()];
        for (position, record) in selected.into_iter().flatten() {
            records[position].push(record);
        }
        Ok(requests
            .iter()
            .zip(headers)
            .zip(records)
            .map(|((request, header), records)| {
                header.map(|header| {
                    MoleculeHashRange::from_write_records(
                        request.molecule.to_owned(),
                        header.version,
                        header.updated_at,
                        records,
                    )
                })
            })
            .collect())
    }

    async fn load_write_scope(
        &self,
        scope: Option<&str>,
        slots: Vec<Slot>,
    ) -> Result<Vec<(usize, Record)>, SchemaError> {
        let mut records = Vec::new();
        let mut slots = slots.into_iter();
        loop {
            let chunk: Vec<_> = slots.by_ref().take(READ_BATCH_KEYS).collect();
            if chunk.is_empty() {
                break;
            }
            let keys: Vec<_> = chunk.iter().map(|slot| slot.key.clone()).collect();
            let values = self.load_visible_per_key_records(scope, &keys).await?;
            if values.len() != chunk.len() {
                return Err(alignment_error());
            }
            for (slot, value) in chunk.into_iter().zip(values) {
                if let Some(value) = value {
                    records.push((
                        slot.request,
                        (slot.hash, slot.range, value.entry, value.meta),
                    ));
                }
            }
        }
        Ok(records)
    }
}

fn alignment_error() -> SchemaError {
    SchemaError::StorageRead {
        detail: "keyed molecule write batch returned a different slot count".into(),
    }
}
