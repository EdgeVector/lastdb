use super::*;
use crate::db_operations::atom_store::{counter::TipChangeSet, AtomStore};
use crate::schema::types::field::build_storage_key;
use crate::schema::{Schema, SchemaError};
use serde_json::json;

impl AtomStore {
    pub(crate) async fn bind_history_schema(&self, schema: &Schema) -> Result<(), SchemaError> {
        self.bind_history_schemas(std::iter::once(schema)).await
    }

    pub(crate) async fn bind_history_schemas<'a>(
        &self,
        schemas: impl Iterator<Item = &'a Schema> + Send,
    ) -> Result<(), SchemaError> {
        let rows = schemas
            .flat_map(|schema| {
                schema
                    .field_molecule_uuids
                    .as_ref()
                    .into_iter()
                    .flatten()
                    .filter_map(|(name, molecule)| {
                        let field = schema.field_hashes.get(name)?;
                        Some((
                            format!("ahf:v1:{molecule}"),
                            json!(HistoryBinding {
                                schema: schema.name.clone(),
                                field: field.clone(),
                                name: name.clone(),
                            }),
                        ))
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        self.commit_tip_changes(TipChangeSet {
            rows,
            ..TipChangeSet::default()
        })
        .await?;
        Ok(())
    }

    pub async fn field_history_policy(
        &self,
        field: &str,
        scope: Option<&str>,
    ) -> Result<FieldHistoryPolicy, SchemaError> {
        let key = build_storage_key(scope, &format!("ahp:v1:{field}"));
        Ok(self.main_store.get_item(&key).await?.unwrap_or_default())
    }

    pub async fn set_field_history_policy(
        &self,
        field: &str,
        policy: FieldHistoryPolicy,
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        if let FieldHistoryPolicy::Ttl { seconds } = policy {
            if seconds == 0 || seconds.checked_mul(1_000_000_000).is_none() {
                return Err(SchemaError::InvalidData(
                    "history TTL must be positive and fit nanoseconds".into(),
                ));
            }
        }
        if field.len() != 64 || !field.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(SchemaError::InvalidData(
                "history needs a canonical field identity".into(),
            ));
        }
        self.in_background_scope(async {
            let mut writes = vec![crate::storage::KvMutation::put(
                build_storage_key(scope, &format!("ahp:v1:{field}")).into_bytes(),
                serde_json::to_vec(&policy).map_err(|e| SchemaError::InvalidData(e.to_string()))?,
            )];
            if policy != FieldHistoryPolicy::Off {
                writes.push(crate::storage::KvMutation::put(
                    Self::CURRENT_KEY_FORMAT_KEY.as_bytes().to_vec(),
                    crate::atom::molecule_key_codec::HISTORY_KEY_FORMAT.to_vec(),
                ));
            }
            self.main_store.inner().batch_mutate(writes).await?;
            Ok(())
        })
        .await
    }
}
