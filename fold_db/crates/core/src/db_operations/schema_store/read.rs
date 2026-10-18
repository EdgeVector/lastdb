//! Exact schema and state reads, including the existing catalog repair path.

use super::*;
use crate::crypto::at_rest::is_sealed_at_rest;

impl SchemaStore {
    /// Get a specific schema by name.
    ///
    /// Query falls back here on a cache miss (`get_schema_following_supersession_for_read`).
    /// A serde failure on an `ENC:` tip used to become HTTP 400
    /// `expected value at line 1 column 1` and blind `kanban list`. Open the
    /// envelope when we have the content key; otherwise treat the row as
    /// missing (same as boot-skip) — never return the serde error.
    pub async fn get_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        let Some(bytes) = self
            .schemas_store
            .inner()
            .get(schema_name.as_bytes())
            .await?
        else {
            return Ok(None);
        };

        let was_sealed = is_sealed_at_rest(&bytes);
        let mut schema = match decode_catalog_schema(&bytes, self.catalog_unwrap_key.as_ref()) {
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

        if was_sealed {
            // Heal the tip so the next boot/get is JSON. Failure is non-fatal:
            // we already have a usable schema for this request.
            if let Err(e) = self.schemas_store.put_item(schema_name, &schema).await {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "opened ENC: catalog tip but could not persist the JSON heal"
                );
            } else if let Err(e) = self.schemas_store.inner().flush().await {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "opened ENC: catalog tip; JSON heal flush failed"
                );
            }
        }

        Ok(Some(schema))
    }

    /// Get the state of a specific schema.
    ///
    /// Same contract as [`Self::get_schema`] and the boot scan
    /// ([`Self::get_all_schema_states`]): an `ENC:` tip opens with the catalog
    /// unwrap key and heals to JSON; a row that still does not decode is
    /// treated as missing and logged, never returned as a serde error.
    ///
    /// Every hard-erasure finalize reloads its schema through
    /// `load_schema_internal`, which reads this row. A strict read here made
    /// every Delete on a schema with a sealed state tip fail with
    /// `Serialization error: expected value at line 1 column 1` after its
    /// destructive step, on every retry, until the persist lane quarantined it
    /// (both BoardCards_hashrange_v1 schemas on the primary, 2026-09-24; their
    /// state tips were written `ENC:` by the 2026-08-16 mis-wired sealer).
    pub async fn get_schema_state(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaState>, SchemaError> {
        let mut found = None;
        for form in crate::kind_partition::read_forms(schema_name) {
            if let Some(bytes) = self
                .schema_states_store
                .inner()
                .get(form.as_bytes())
                .await?
            {
                found = Some(bytes);
                break;
            }
        }
        let Some(bytes) = found else {
            return Ok(None);
        };
        let was_sealed = is_sealed_at_rest(&bytes);
        let state =
            match decode_catalog_json::<SchemaState>(&bytes, self.catalog_unwrap_key.as_ref()) {
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
        if was_sealed {
            // Heal so the next read is plain JSON. Non-fatal: this request
            // already has a usable state.
            if let Err(e) = self.schema_states_store.put_item(schema_name, &state).await {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "opened ENC: schema state tip but could not persist the JSON heal"
                );
            } else if let Err(e) = self.schema_states_store.inner().flush().await {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "opened ENC: schema state tip; JSON heal flush failed"
                );
            }
        }
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
