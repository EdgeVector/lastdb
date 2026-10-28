//! Exact schema and state reads, including the existing catalog repair path.

use super::*;

impl SchemaStore {
    /// Get a specific schema by name.
    ///
    /// Query falls back here on a cache miss (`get_schema_following_supersession_for_read`).
    /// A row that does not decode is treated as missing (same as boot-skip)
    /// and logged — never returned as a serde error, which would become HTTP
    /// 400 `expected value at line 1 column 1` and blind `kanban list`.
    pub async fn get_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        if let Some(snapshot) = self.prepared_snapshot(schema_name)? {
            return Ok(snapshot.schema.clone());
        }
        let Some(bytes) = self
            .schemas_store
            .inner()
            .get(schema_name.as_bytes())
            .await?
        else {
            return Ok(None);
        };

        let mut schema = match decode_catalog_schema(&bytes) {
            Ok(schema) => schema,
            Err(e) => {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "durable schema row is unreadable; treating as missing so \
                     query does not 400 (boot-skip already left it out of cache)"
                );
                return Ok(None);
            }
        };
        schema.populate_runtime_fields()?;

        Ok(Some(schema))
    }

    /// Get the state of a specific schema.
    ///
    /// Same contract as [`Self::get_schema`] and the boot scan
    /// ([`Self::get_all_schema_states`]): a row that does not decode is
    /// treated as missing and logged, never returned as a serde error.
    ///
    /// Every hard-erasure finalize reloads its schema through
    /// `load_schema_internal`, which reads this row. A strict read here would
    /// make every Delete on a schema with an unreadable state row fail with
    /// `Serialization error: expected value at line 1 column 1` after its
    /// destructive step, on every retry, until the persist lane quarantined it.
    pub async fn get_schema_state(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaState>, SchemaError> {
        let Some(bytes) = self
            .schema_states_store
            .inner()
            .get(schema_name.as_bytes())
            .await?
        else {
            return Ok(None);
        };
        let state = match decode_catalog_json::<SchemaState>(&bytes) {
            Ok(state) => state,
            Err(e) => {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "durable schema state row is unreadable; treating it as missing \
                     (boot-skip already left it out of the state cache)"
                );
                return Ok(None);
            }
        };
        Ok(Some(state))
    }

    pub(super) async fn require_installed_schema(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        if self.get_schema(schema_name).await?.is_some() {
            Ok(())
        } else {
            Err(SchemaError::NotFound(schema_name.to_string()))
        }
    }
}
