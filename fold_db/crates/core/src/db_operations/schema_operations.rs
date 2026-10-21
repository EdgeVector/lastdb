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
            .validate_field_names()
            .map_err(SchemaError::InvalidData)?;
        let old = self.schemas().get_schema(schema_name).await?;
        let old_edges = old.as_ref().map_or_else(Vec::new, |existing| {
            schema_molecule_ref_edges(schema_name, existing)
        });
        let new_edges = schema_molecule_ref_edges(schema_name, schema);
        self.refuse_while_draining(schema_name, &old_edges, &new_edges)
            .await?;
        self.molecule_keys().ensure_schema(schema).await?;
        // Bind first. Each new binding record and the counter of its molecule
        // are one batch, and a binding that already exists changes nothing.
        let bound = self.atoms().add_molecule_bindings(&new_edges, None).await?;
        if bound != 0 {
            self.atoms().flush().await?;
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
        let stale: Vec<_> = old_edges
            .into_iter()
            .filter(|old| !new_edges.contains(old))
            .collect();
        self.atoms().remove_molecule_bindings(&stale, None).await?;
        if !stale.is_empty() {
            self.release_protein_members_of(&stale).await?;
        }
        Ok(())
    }

    /// Refuse an install that would bind a molecule again while the molecule drain
    /// still removes its keys. A molecule that the schema already binds is live and
    /// is not checked.
    async fn refuse_while_draining(
        &self,
        schema_name: &str,
        old_edges: &[MoleculeRefEdge],
        new_edges: &[MoleculeRefEdge],
    ) -> Result<(), SchemaError> {
        let mut added: Vec<String> = new_edges
            .iter()
            .filter(|edge| {
                !old_edges
                    .iter()
                    .any(|old| old.molecule_uuid == edge.molecule_uuid)
            })
            .map(|edge| edge.molecule_uuid.clone())
            .collect();
        added.sort();
        added.dedup();
        if added.is_empty() {
            return Ok(());
        }
        let found = self.atoms().molecules_draining(None, &added).await?;
        let molecules = found.erasing + found.waiting_records;
        if molecules == 0 {
            return Ok(());
        }
        let reason = if self.atoms().drain_task_is_off() {
            "drain_task_off"
        } else if found.erasing > 0 {
            "draining"
        } else {
            "waiting_for_conflict_records"
        };
        Err(SchemaError::SchemaBeingDropped {
            schema: schema_name.to_string(),
            reason: reason.to_string(),
            molecules,
        })
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

    pub fn drop_schema<'a>(
        &'a self,
        schema_name: &'a str,
    ) -> BoxFuture<'a, Result<bool, SchemaError>> {
        crate::storage::disk_pack::transaction::scope(
            self.namespaced_store().disk_pack_runtime(),
            self.drop_schema_in_scope(schema_name),
        )
        .boxed()
    }

    async fn drop_schema_in_scope(&self, schema_name: &str) -> Result<bool, SchemaError> {
        let old = self.schemas().get_schema(schema_name).await?;
        let old_edges = old.as_ref().map_or_else(Vec::new, |existing| {
            schema_molecule_ref_edges(schema_name, existing)
        });
        let existed = self.schemas().drop_schema(schema_name).await?;
        // The schema record goes first. Each binding record and the counter of
        // its molecule leave in one batch.
        self.atoms()
            .remove_molecule_bindings(&old_edges, None)
            .await?;
        self.release_protein_members_of(&old_edges).await?;
        Ok(existed)
    }

    /// A molecule that only a protein still names is released from it, so its
    /// `bindings` counter can reach zero and the molecule drain can take it.
    async fn release_protein_members_of(
        &self,
        edges: &[MoleculeRefEdge],
    ) -> Result<(), SchemaError> {
        let mut molecules: Vec<String> = edges
            .iter()
            .map(|edge| edge.molecule_uuid.clone())
            .collect();
        molecules.sort();
        molecules.dedup();
        let atoms = self.atoms();
        atoms
            .with_protein_membership(atoms.protein_release_members(&molecules))
            .await
            .map(|_| ())
    }

    pub async fn get_schema_drop_receipt(
        &self,
        schema_name: &str,
    ) -> Result<Option<crate::db_operations::schema_store::SchemaDropReceipt>, SchemaError> {
        self.schemas().get_schema_drop_receipt(schema_name).await
    }
}
