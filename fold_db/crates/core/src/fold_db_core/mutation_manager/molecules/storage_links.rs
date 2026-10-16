//! Exact storage aliases for the dirty graph's API-form tip slots.

use std::collections::HashMap;

use crate::schema::SchemaError;

use super::super::MutationManager;
use super::scoped_gate::schema_is_scoped;
use super::PreparedSchemaDelta;

pub(in crate::fold_db_core::mutation_manager) type StorageTipAliases =
    HashMap<(String, String, String), Vec<String>>;

impl MutationManager {
    /// Encode every possible publication before the first resident change.
    /// Apply may reduce this superset after the per-delta LWW rebase.
    pub(in crate::fold_db_core::mutation_manager) fn prepare_resident_storage_links(
        &self,
        deltas: &[PreparedSchemaDelta],
    ) -> Result<Vec<StorageTipAliases>, SchemaError> {
        deltas
            .iter()
            .map(|delta| {
                let mut links = StorageTipAliases::new();
                if schema_is_scoped(&delta.schema) {
                    return Ok(links);
                }
                let direct = delta.changed_keys.iter().filter_map(|(name, changed)| {
                    let field = delta.schema.runtime_fields.get(name)?;
                    Some((field.common().molecule_uuid()?.as_str(), changed))
                });
                let siblings = delta
                    .sibling_updates
                    .iter()
                    .map(|(uuid, _, changed)| (uuid.as_str(), changed));
                for (uuid, changed) in direct.chain(siblings) {
                    let codec = self.db_ops.atoms().key_codec_for_molecule(uuid);
                    for key in changed {
                        let hash = key.disk_hash();
                        let range = key.disk_range();
                        let aliases = codec
                            .api_hash_range_record_keys_for_read(uuid, hash, range)
                            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
                        links.insert(
                            (uuid.to_string(), hash.to_string(), range.to_string()),
                            aliases,
                        );
                    }
                }
                Ok(links)
            })
            .collect()
    }
}
