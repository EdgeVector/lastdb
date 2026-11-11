//! Per-schema record groups share one complete native commit.

use super::*;

type KeyGroups = Vec<(String, Vec<AuthoredMutation>)>;

impl MutationManager {
    /// Stage every group's conditional replacements before publication.
    pub(in crate::fold_db_core::mutation_manager::write) async fn stage_record_groups(
        &self,
        mutations: Vec<AuthoredMutation>,
        stages: &mut ResidentCommitStages,
        runtime: &std::sync::Arc<crate::storage::disk_pack::PackRuntime>,
    ) -> Result<usize, SchemaError> {
        let grouped_mutations =
            self.independent_key_groups(self.group_mutations_by_schema(mutations))?;

        let group_count = grouped_mutations.len();
        let mut prepare_elapsed = std::time::Duration::ZERO;
        for (schema_name, schema_mutations) in grouped_mutations {
            let prepare_started = std::time::Instant::now();
            self.stage_record_group(&schema_name, schema_mutations, runtime)
                .await?;
            prepare_elapsed += prepare_started.elapsed();
        }
        stages.prepare_us = u64::try_from(prepare_elapsed.as_micros()).unwrap_or(u64::MAX);
        Ok(group_count)
    }
}

/// Independent records prepare together. A repeated primary key is rejected:
/// its replacement would require the revision of an unacknowledged write.
impl MutationManager {
    fn independent_key_groups(&self, groups: KeyGroups) -> Result<KeyGroups, SchemaError> {
        let mut result = Vec::new();
        for (name, mutations) in groups {
            let schema = self.schema_manager.require_schema_metadata(&name)?;
            let mut current_keys = HashSet::new();
            let mut current = Vec::new();
            for mutation in mutations {
                let key = self.resolve_mutation_key_value(&name, &schema, &mutation.mutation)?;
                if current_keys.contains(&key) {
                    return Err(SchemaError::InvalidData(
                        "a batch cannot replace the same record twice".into(),
                    ));
                }
                current_keys.insert(key);
                current.push(mutation);
            }
            if !current.is_empty() {
                result.push((name, current));
            }
        }
        Ok(result)
    }
}
