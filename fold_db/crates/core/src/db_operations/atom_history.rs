//! Resolve a schema field and API key to one exact history location.
use super::atom_store::atom_history::{FieldHistoryPolicy, HistoryLocation};
use super::DbOperations;
use crate::schema::SchemaError;

pub struct AtomHistoryTarget {
    pub field_identity: String,
    pub slot: String,
}

impl DbOperations {
    pub async fn atom_history_target(
        &self,
        location: &HistoryLocation,
        scope: Option<&str>,
    ) -> Result<AtomHistoryTarget, SchemaError> {
        let schema = self
            .get_schema(&location.schema)
            .await?
            .ok_or_else(|| SchemaError::InvalidData("history schema does not exist".into()))?;
        let field_identity = schema
            .field_hashes
            .get(&location.field)
            .cloned()
            .ok_or_else(|| SchemaError::InvalidData("history field does not exist".into()))?;
        let molecule = schema
            .field_molecule_uuids
            .as_ref()
            .and_then(|m| m.get(&location.field))
            .ok_or_else(|| SchemaError::InvalidData("history field has no molecule".into()))?;
        let slot = self
            .atoms()
            .atom_history_slot(molecule, &location.key, scope)
            .await?;
        Ok(AtomHistoryTarget {
            field_identity,
            slot,
        })
    }

    pub async fn configure_atom_history(
        &self,
        target: &AtomHistoryTarget,
        policy: FieldHistoryPolicy,
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        let schemas = self.get_all_schemas().await?;
        self.atoms()
            .in_background_scope(async {
                self.atoms().bind_history_schemas(schemas.values()).await?;
                self.atoms()
                    .set_field_history_policy(&target.field_identity, policy, scope)
                    .await
            })
            .await
    }
}
