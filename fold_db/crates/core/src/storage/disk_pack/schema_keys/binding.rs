//! Resolve only the membership field and scope that the query actually uses.

use super::*;
use crate::schema::types::field::{FieldKind, FieldVariant};
use crate::storage::disk_pack::logical_paths::field::FieldScope;

impl PackRuntime {
    /// A fallback field needs complete keys only when it establishes the rows.
    pub(crate) async fn preload_bound_field<S: SchemaKeySource>(
        self: &Arc<Self>,
        db_ops: Arc<S>,
        schema: &Schema,
        name: &str,
    ) -> Result<(), SchemaError> {
        if !self.schema_keys.selected(schema) {
            return Ok(());
        }
        let Some(field) = schema.runtime_fields.get(name) else {
            return Ok(());
        };
        self.preload_field_binding(db_ops, field, field.common().storage_prefix())
            .await
    }

    /// Share resolution runs inside the primary field's current binding.
    pub(crate) async fn preload_current_field<S: SchemaKeySource>(
        self: &Arc<Self>,
        db_ops: Arc<S>,
        field: &FieldVariant,
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        let Some(binding) = FieldScope::current() else {
            return Ok(());
        };
        let (name, hash) = binding.schema_binding_identity()?;
        if !self
            .schema_keys
            .policy
            .iter()
            .any(|selected| selected == "*" || selected == &name || hash.as_ref() == Some(selected))
        {
            return Ok(());
        }
        self.preload_field_binding(db_ops, field, scope).await
    }

    async fn preload_field_binding<S: SchemaKeySource>(
        self: &Arc<Self>,
        db_ops: Arc<S>,
        field: &FieldVariant,
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        if field.kind != FieldKind::HashRange || transaction::PendingBatch::current(self).is_some()
        {
            return Ok(());
        }
        let Some(molecule) = field.common().molecule_uuid() else {
            return Ok(());
        };
        self.preload_molecule_keys(db_ops, scope, molecule).await
    }
}
