//! Read exact primary field dependencies in batches within the caller's native view.

use super::*;
use crate::db_operations::atom_store::AtomStore;
use std::collections::BTreeSet;

impl Schema {
    /// The fields that derive the alternate keys of one record, with the primary
    /// hash field. An erase budget counts the same set.
    pub(crate) fn alternate_dependency_fields(&self) -> BTreeSet<&str> {
        self.alternate_keys
            .values()
            .flat_map(schema_types::AlternateKey::fields)
            .chain(self.key.iter().filter_map(|key| key.hash_field.as_deref()))
            .collect()
    }
}

impl AtomStore {
    pub(crate) async fn alternate_fields(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        scope: Option<&str>,
    ) -> Result<Vec<RecordFields>, SchemaError> {
        let names = schema.alternate_dependency_fields();
        self.schema_fields_at_keys(schema, keys, scope, &names)
            .await
    }

    pub(crate) async fn schema_fields_at_keys(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        scope: Option<&str>,
        names: &BTreeSet<&str>,
    ) -> Result<Vec<RecordFields>, SchemaError> {
        if names.len().saturating_mul(keys.len()) > MAX_ALTERNATE_ENTRIES {
            return Err(invalid("alternate field dependencies exceed 4096 records"));
        }
        let mut slots = Vec::new();
        let mut coordinates = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            for name in names {
                let field = schema
                    .runtime_fields
                    .get(*name)
                    .ok_or_else(|| invalid("alternate field is not declared"))?;
                let molecule = field
                    .common()
                    .molecule_uuid()
                    .ok_or_else(|| invalid("alternate source field has no molecule"))?;
                let (hash, range) = field
                    .slot_coordinates(key)
                    .ok_or_else(|| invalid("alternate source key does not match its schema"))?;
                slots.push(
                    self.key_codec_for_molecule(molecule)?
                        .api_hash_range_record_key(molecule, &hash, &range)
                        .map_err(|_| invalid("invalid alternate source key"))?,
                );
                coordinates.push((index, *name));
            }
        }
        let tips = self.load_visible_per_key_records(scope, &slots).await?;
        if tips.len() != slots.len() {
            return Err(invalid("alternate field tip batch is incomplete"));
        }
        let selected: Vec<_> = coordinates
            .into_iter()
            .zip(tips)
            .filter_map(|(coordinate, tip)| tip.map(|tip| (coordinate, tip.entry.atom_uuid)))
            .collect();
        let ids: Vec<_> = selected.iter().map(|(_, id)| id.as_str()).collect();
        let atoms = self.get_atoms(&ids, scope).await?;
        if atoms.len() != selected.len() {
            return Err(invalid("alternate field atom batch is incomplete"));
        }
        let mut fields = vec![BTreeMap::new(); keys.len()];
        for (((index, name), _), atom) in selected.into_iter().zip(atoms) {
            let atom = atom.ok_or(SchemaError::MissingSelectedAtom)?;
            fields[index].insert(name.to_string(), atom.content().clone());
        }
        Ok(fields)
    }
}
