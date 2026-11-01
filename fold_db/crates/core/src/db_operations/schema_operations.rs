//! Logical schema writes establish field keys and liveness before publication.
//! The underlying SchemaStore remains the physical schema-record boundary.

use super::core::DbOperations;
use crate::db_operations::atom_store::MoleculeRefEdge;
use crate::schema::{Schema, SchemaError, SchemaNameClaim, SchemaRetentionPolicy, SchemaState};
use futures::{future::BoxFuture, FutureExt};
use std::collections::HashMap;

mod attribution;
mod attribution_reports;

pub use attribution_reports::{SchemaRetentionAttributionReport, SchemaRootAttributionReport};

fn schema_molecule_ref_edges(schema_name: &str, schema: &Schema) -> Vec<MoleculeRefEdge> {
    let mut fields: Vec<_> = schema
        .field_molecule_uuids
        .as_ref()
        .into_iter()
        .flat_map(|molecules| molecules.iter())
        .map(|(field, molecule)| MoleculeRefEdge::schema_field(schema_name, field, molecule))
        .collect();
    fields.extend(schema.alternate_keys.keys().map(|name| MoleculeRefEdge {
        molecule_uuid: crate::atom::molecule_uuid::alternate_molecule_uuid(schema, name),
        source: format!(
            "alternate:{}",
            serde_json::to_string(&(schema_name, name)).expect("alternate source serializes")
        ),
        edge_type: "schema-alternate-key".to_owned(),
    }));
    fields.sort_by(|left, right| left.source.cmp(&right.source));
    fields
}

impl DbOperations {
    pub async fn get_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        self.schemas().get_schema(schema_name).await
    }

    pub async fn get_schema_state(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaState>, SchemaError> {
        self.schemas().get_schema_state(schema_name).await
    }

    pub fn store_schema<'a>(
        &'a self,
        schema_name: &'a str,
        schema: &'a Schema,
    ) -> BoxFuture<'a, Result<(), SchemaError>> {
        crate::storage::disk_pack::transaction::scope(
            self.namespaced_store().disk_pack_runtime(),
            self.store_schema_in_scope(schema_name, schema),
        )
        .boxed()
    }

    async fn store_schema_in_scope(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        schema
            .validate_definition()
            .map_err(SchemaError::InvalidData)?;
        let old = self.schemas().get_schema(schema_name).await?;
        self.molecule_keys()
            .establish_schema_for_publication(schema, old.as_ref())
            .await?;
        self.schemas().store_schema(schema_name, schema).await?;
        for edge in &schema_molecule_ref_edges(schema_name, schema) {
            self.atoms()
                .keep_small()
                .ensure_molecule_counter(&edge.molecule_uuid, schema_name);
        }
        // Debounced persist only. `store_schema` runs on every schema put and
        // a per-write flush here was one of the 2026-09-21 flush-storm sites.
        let _ = self.atoms().persist_keep_small().await;
        Ok(())
    }

    pub async fn store_schema_state(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        self.schemas().store_schema_state(schema_name, state).await
    }

    pub async fn get_schema_name_claim(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaNameClaim>, SchemaError> {
        self.schemas().get_schema_name_claim(schema_name).await
    }

    pub async fn set_schema_name_claim_retired(
        &self,
        schema_name: &str,
        retired: bool,
    ) -> Result<bool, SchemaError> {
        self.schemas()
            .set_schema_name_claim_retired(schema_name, retired)
            .await
    }

    pub async fn list_retired_name_claims(&self) -> Result<Vec<String>, SchemaError> {
        self.schemas().list_retired_name_claims().await
    }

    pub async fn get_schema_retention_policy(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaRetentionPolicy>, SchemaError> {
        self.schemas()
            .get_schema_retention_policy(schema_name)
            .await
    }

    pub async fn set_schema_retention_policy(
        &self,
        schema_name: &str,
        policy: SchemaRetentionPolicy,
    ) -> Result<(), SchemaError> {
        self.schemas()
            .set_schema_retention_policy(schema_name, policy)
            .await
    }

    pub async fn clear_schema_retention_policy(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        self.schemas()
            .clear_schema_retention_policy(schema_name)
            .await
    }

    pub async fn list_schema_retention_policies(
        &self,
    ) -> Result<Vec<(String, SchemaRetentionPolicy)>, SchemaError> {
        self.schemas().list_schema_retention_policies().await
    }

    pub async fn get_all_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        self.schemas().get_all_schemas().await
    }

    pub async fn get_all_superseded_by(&self) -> Result<HashMap<String, String>, SchemaError> {
        self.schemas().get_all_superseded_by().await
    }

    pub async fn get_all_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
        self.schemas().get_all_schema_states().await
    }

    /// Remove one installed schema: release the rows it stored, then remove
    /// its schema record. Returns whether the schema was installed.
    ///
    /// The release goes first, in bounded batches with one write scope each
    /// (`AtomStore::release_schema_rows`): the records, their conflict rows,
    /// the field history and the alternate entries of the schema, with their
    /// blob counts. A schema installed again under the same name gets the same
    /// record space, so a record that the drop left would come back in it. A
    /// release that stops leaves the schema record, and the next drop goes on.
    /// Then one write scope removes the schema record.
    pub fn drop_schema<'a>(
        &'a self,
        schema_name: &'a str,
    ) -> BoxFuture<'a, Result<bool, SchemaError>> {
        async move {
            if let Some(schema) = self.schemas().get_schema(schema_name).await? {
                let released = self.atoms().release_schema_rows(&schema).await?;
                tracing::info!(
                    target: "fold_node::database",
                    schema = schema_name,
                    records = released.records,
                    history = released.history,
                    alternates = released.alternates,
                    "schema drop released the stored rows of the schema"
                );
            }
            crate::storage::disk_pack::transaction::scope(
                self.namespaced_store().disk_pack_runtime(),
                self.schemas().drop_schema(schema_name),
            )
            .await
        }
        .boxed()
    }

    pub async fn get_schema_drop_receipt(
        &self,
        schema_name: &str,
    ) -> Result<Option<crate::db_operations::schema_store::SchemaDropReceipt>, SchemaError> {
        self.schemas().get_schema_drop_receipt(schema_name).await
    }
}
