//! A bounded name directory belongs only to one captured native query.

use super::*;
use crate::schema::{SchemaNameClaim, SchemaWithState};

const MAX_NAMES: usize = 10_000;

impl SchemaStore {
    pub(crate) async fn read_name_directory(&self) -> Result<Vec<SchemaWithState>, SchemaError> {
        let schemas = self.get_all_schemas_strict().await?;
        if schemas.len() > MAX_NAMES {
            return Err(SchemaError::StorageRead {
                detail: "captured schema name directory exceeds its call limit".into(),
            });
        }
        let mut keys = Vec::new();
        let mut entries = Vec::with_capacity(schemas.len());
        for schema in schemas.into_values() {
            let start = keys.len();
            keys.extend(
                crate::kind_partition::read_forms(&schema.name)
                    .into_iter()
                    .map(String::into_bytes),
            );
            let claim = keys.len();
            keys.push(name_claim_key(&schema.name).into_bytes());
            entries.push((schema, start..claim, claim));
        }
        let count = keys.len();
        let values = self.schema_states_store.inner().get_many(keys).await?;
        if values.len() != count {
            return Err(SchemaError::StorageRead {
                detail: "captured schema state batch returned the wrong slot count".into(),
            });
        }
        let mut directory = Vec::with_capacity(entries.len());
        for (schema, states, claim) in entries {
            let state = states
                .into_iter()
                .find_map(|slot| values[slot].as_deref())
                .map(|bytes| self.decode_name_value::<SchemaState>(bytes))
                .transpose()?
                .unwrap_or_default();
            let retired = values[claim]
                .as_deref()
                .map(|bytes| self.decode_name_value::<SchemaNameClaim>(bytes))
                .transpose()?
                .is_some_and(|claim| claim.retired);
            if !retired {
                directory.push(SchemaWithState::new(schema, state));
            }
        }
        Ok(directory)
    }

    fn decode_name_value<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, SchemaError> {
        decode_catalog_json(bytes, self.catalog_unwrap_key.as_ref())
            .map_err(|detail| SchemaError::StorageRead { detail })
    }
}
