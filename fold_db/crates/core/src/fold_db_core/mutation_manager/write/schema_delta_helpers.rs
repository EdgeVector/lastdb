//! Helpers that prepare a native schema delta: schema load, share prefixes, and dedupe.

use super::*;

impl MutationManager {
    /// Load the schema with its runtime fields and fold mutations into the record envelope.
    pub(super) async fn load_schema_for_delta(
        &self,
        schema_name: &str,
        schema_mutations: &mut [Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<crate::schema::types::Schema, SchemaError> {
        let snapshot = self.db_ops.schemas().read_snapshot(schema_name).await?;
        if snapshot.state == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{schema_name}' is blocked and cannot be mutated"
            )));
        }
        let mut schema = snapshot
            .schema
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        apply_storage_prefix_to_schema(&mut schema, storage_prefix);
        schema.ensure_record_molecule_runtime_field();
        crate::record_molecule::fold_mutations_to_record_envelope(
            &self.db_ops,
            &schema,
            schema_mutations,
        )
        .await?;
        Ok(schema)
    }

    /// Active share-rule prefixes that apply to this schema (personal namespace only).
    pub(super) async fn active_share_prefixes(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        if storage_prefix.is_some() {
            return Ok(Vec::new());
        }
        Ok(crate::sharing::store::list_share_rules_in_ops(&self.db_ops)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load share rules for schema '{schema_name}': {error}"
                ))
            })?
            .into_iter()
            .filter(|rule| rule.active && rule.scope_matches(schema_name))
            .map(|rule| rule.share_prefix)
            .collect())
    }

    /// Drop unchanged field writes when write dedupe is enabled.
    pub(super) fn dedupe_atom_results(
        &self,
        schema_name: &str,
        schema: &crate::schema::types::Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[crate::schema::types::KeyValue],
        atom_results: Vec<(usize, String, crate::atom::Atom)>,
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
    ) -> Vec<(usize, String, crate::atom::Atom)> {
        if !self.write_dedupe_enabled() {
            return atom_results;
        }
        let dedupe_start = std::time::Instant::now();
        let sent = atom_results.len();
        let kept = Self::drop_unchanged_field_writes(
            schema,
            schema_mutations,
            mutation_key_values,
            atom_results,
        );
        Self::add_timing(timing_breakdown, "  - dedupe_scan", dedupe_start.elapsed());
        if kept.len() != sent {
            tracing::debug!(
                target: "write_dedupe",
                schema = %schema_name,
                sent,
                written = kept.len(),
                skipped = sent - kept.len(),
                "dropped unchanged field writes"
            );
        }
        kept
    }
}
