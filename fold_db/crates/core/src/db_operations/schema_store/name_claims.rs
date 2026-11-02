//! Name-claim methods of [`SchemaStore`].

use super::catalog_keys::*;
use super::SchemaStore;
use crate::schema::{SchemaError, SchemaNameClaim};

impl SchemaStore {
    /// Retire or restore an installed schema's claim on its `descriptive_name`.
    ///
    /// Returns `true` when the durable record changed, so a caller can report
    /// an idempotent no-op honestly instead of claiming it retired something.
    ///
    /// This never touches the schema artifact, its `identity_hash`, or its
    /// [`SchemaState`]. A retired claimant stays readable by canonical name and
    /// by identity hash; it just stops answering `descriptive_name` lookups.
    pub async fn set_schema_name_claim_retired(
        &self,
        schema_name: &str,
        retired: bool,
    ) -> Result<bool, SchemaError> {
        self.require_installed_schema(schema_name).await?;
        let key = name_claim_key(schema_name);
        let claim = SchemaNameClaim { retired };
        if self
            .schema_states_store
            .get_item::<SchemaNameClaim>(&key)
            .await
            .unwrap_or(None)
            == Some(claim)
        {
            return Ok(false);
        }
        self.schema_states_store.put_item(&key, &claim).await?;
        self.schema_states_store.inner().flush().await?;
        Ok(true)
    }

    /// Every installed schema whose name claim is retired, sorted.
    ///
    /// Read once at boot by `SchemaCore::new`, exactly like the schema-state
    /// and superseded-by maps.
    pub async fn list_retired_name_claims(&self) -> Result<Vec<String>, SchemaError> {
        let scan = self
            .schema_states_store
            .scan_items_with_prefix_partition_undecodable::<SchemaNameClaim>(NAME_CLAIM_KEY_PREFIX)
            .await?;
        for (key, error) in &scan.undecodable {
            tracing::warn!(
                key = %key,
                error = %error,
                "unreadable schema name-claim row; skipping"
            );
        }
        let mut out = Vec::new();
        for (key, claim) in scan.items {
            if !claim.retired {
                continue;
            }
            let Some(schema_name) = key.strip_prefix(NAME_CLAIM_KEY_PREFIX) else {
                continue;
            };
            if schema_name.is_empty() {
                continue;
            }
            out.push(schema_name.to_string());
        }
        out.sort();
        Ok(out)
    }
}
