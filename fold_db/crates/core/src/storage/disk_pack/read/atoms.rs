//! Exact physical body reads can reuse the captured typed values.

use super::super::*;
use crate::atom::Atom;

impl PackRuntime {
    pub(crate) fn captured_atoms(
        self: &Arc<Self>,
        keys: &[String],
    ) -> StorageResult<Vec<Option<Arc<Atom>>>> {
        if transaction::PendingBatch::current(self).is_some() {
            return Ok(vec![None; keys.len()]);
        }
        let Some(context) = super::ReadContext::captured(self) else {
            return Ok(vec![None; keys.len()]);
        };
        let requested: Vec<_> = keys
            .iter()
            .map(|key| {
                let (collection, encoded) = kv::coordinate("main", key.as_bytes());
                (
                    collection,
                    encoded,
                    crate::atom::atom_key_codec::body_uuid_of(key)
                        .unwrap_or_default()
                        .to_owned(),
                )
            })
            .collect();
        let values = context.core_model.atoms_for_sources(&requested);
        let mut call = crate::read_receipts::lock(&context.call);
        let mut atoms = Vec::with_capacity(keys.len());
        for (key, value) in keys.iter().zip(values) {
            let Some(value) = value else {
                atoms.push(None);
                continue;
            };
            context.account_memory_record(&mut call, key.len(), value.bytes)?;
            atoms.push(Some(value.atom));
        }
        drop(call);
        for ((key, atom), (collection, encoded, _)) in keys.iter().zip(&atoms).zip(requested) {
            if atom.is_some() {
                logical_paths::source::note_read_source(
                    Arc::clone(self),
                    key.as_bytes(),
                    Some((&collection, &encoded)),
                )?;
            }
        }
        Ok(atoms)
    }
}
