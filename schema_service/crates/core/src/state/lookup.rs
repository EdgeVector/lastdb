//! Read-through lookup for schemas committed by another service instance.

use super::*;
use std::collections::hash_map::Entry;

impl SchemaServiceState {
    /// A warm process can lack an identity that another instance acknowledged.
    /// Consult durable definitions on a miss; never replace concurrent cache entries.
    pub async fn lookup_schema_by_name(&self, name: &str) -> FoldDbResult<Option<Schema>> {
        if let Some(schema) = self.get_schema_by_name(name)? {
            return Ok(Some(schema));
        }
        let stored = self.storage.backend().load_all_schemas().await?;
        let schema = match stored.get(name) {
            Some(schema) => Some(schema.clone()),
            None => Self::stored_schema_by_description(&stored, name)?,
        };
        if let Some(schema) = &schema {
            self.validate_loaded_schemas(&HashMap::from([(schema.name.clone(), schema.clone())]))
                .await?;
            let inserted = {
                let mut schemas = write_lock(&self.schemas, "schemas")?;
                if let Entry::Vacant(entry) = schemas.entry(schema.name.clone()) {
                    entry.insert(schema.clone());
                    self.bump_state_version();
                    true
                } else {
                    false
                }
            };
            if inserted {
                self.rebuild_descriptive_name_index();
            }
        }
        Ok(schema)
    }

    fn stored_schema_by_description(
        stored: &HashMap<String, Schema>,
        name: &str,
    ) -> FoldDbResult<Option<Schema>> {
        let mut matches = stored.values().filter(|schema| {
            schema.superseded_by.is_none()
                && schema
                    .descriptive_name
                    .as_deref()
                    .is_some_and(|description| {
                        descriptive_name_key(schema.owner_app_id.as_deref(), description) == name
                    })
        });
        let schema = matches.next().cloned();
        if matches.next().is_some() {
            return Err(FoldDbError::Config(
                "stored schema descriptive name is ambiguous".to_owned(),
            ));
        }
        Ok(schema)
    }
}
