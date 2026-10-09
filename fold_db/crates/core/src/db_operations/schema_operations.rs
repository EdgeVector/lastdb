//! Thin delegators forwarding `DbOperations::get_schema` etc. to the
//! underlying `SchemaStore`. New code should prefer `db_ops.schemas()`
//! directly.

use super::core::DbOperations;
use crate::db_operations::atom_store::MoleculeRefEdge;
use crate::schema::{Schema, SchemaError, SchemaNameClaim, SchemaRetentionPolicy, SchemaState};
use std::collections::HashMap;

mod attribution;
mod attribution_reports;
mod liveness_bootstrap;
mod molecule_measure;

pub use attribution_reports::{
    SchemaRetentionAttributionReport, SchemaRootAttributionReport, SchemaRootTipAttributionReport,
};
pub use liveness_bootstrap::LivenessBootstrapReport;

fn schema_molecule_ref_edges(schema_name: &str, schema: &Schema) -> Vec<MoleculeRefEdge> {
    let mut fields: Vec<_> = schema
        .field_molecule_uuids
        .as_ref()
        .into_iter()
        .flat_map(|molecules| molecules.iter())
        .map(|(field, molecule)| MoleculeRefEdge::schema_field(schema_name, field, molecule))
        .collect();
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

    pub async fn store_schema(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        let old = self.schemas().get_schema(schema_name).await?;
        let old_edges = old.as_ref().map_or_else(Vec::new, |existing| {
            schema_molecule_ref_edges(schema_name, existing)
        });
        let new_edges = schema_molecule_ref_edges(schema_name, schema);
        let molecule_targets: Vec<String> = old_edges
            .iter()
            .chain(&new_edges)
            .map(|edge| edge.molecule_uuid.clone())
            .collect();
        let _target_gates = self
            .atoms()
            .lock_liveness_molecules(&molecule_targets)
            .await;

        // Retain first. Schema rows and liveness edges live in separate
        // namespaces, so an interrupted write may leave an extra edge but can
        // never publish a catalog binding without its retaining edge.
        for edge in &new_edges {
            self.atoms().put_molecule_ref_edge(edge, None).await?;
        }
        self.schemas().store_schema(schema_name, schema).await?;
        for edge in &new_edges {
            self.atoms()
                .keep_small()
                .ensure_molecule_counter(&edge.molecule_uuid, schema_name);
        }
        // Debounced persist only. `store_schema` runs on every schema put and
        // a per-write flush here was one of the 2026-09-21 flush-storm sites.
        let _ = self.atoms().persist_keep_small().await;
        for edge in old_edges.into_iter().filter(|old| !new_edges.contains(old)) {
            self.atoms().delete_molecule_ref_edge(&edge, None).await?;
        }
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

    pub async fn store_superseded_by(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), SchemaError> {
        self.schemas().store_superseded_by(old_name, new_name).await
    }

    pub async fn get_all_superseded_by(&self) -> Result<HashMap<String, String>, SchemaError> {
        self.schemas().get_all_superseded_by().await
    }

    pub async fn get_all_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
        self.schemas().get_all_schema_states().await
    }

    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        let old = self.schemas().get_schema(schema_name).await?;
        let old_edges = old.as_ref().map_or_else(Vec::new, |existing| {
            schema_molecule_ref_edges(schema_name, existing)
        });
        let molecule_targets: Vec<String> = old_edges
            .iter()
            .map(|edge| edge.molecule_uuid.clone())
            .collect();
        let _target_gates = self
            .atoms()
            .lock_liveness_molecules(&molecule_targets)
            .await;
        let existed = self.schemas().drop_schema(schema_name).await?;
        // Source removal is durable before an edge leaves the active set.
        // A failed delete retains the molecule for the later bootstrap/audit.
        for edge in old_edges {
            self.atoms().delete_molecule_ref_edge(&edge, None).await?;
        }
        Ok(existed)
    }

    pub async fn get_schema_drop_receipt(
        &self,
        schema_name: &str,
    ) -> Result<Option<crate::db_operations::schema_store::SchemaDropReceipt>, SchemaError> {
        self.schemas().get_schema_drop_receipt(schema_name).await
    }
}
