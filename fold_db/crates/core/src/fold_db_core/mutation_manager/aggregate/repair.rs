//! Aggregate summary bounded-repair path.

use super::*;

mod entry;
mod rules;
use super::rules::summary_mutation;
use crate::db_operations::field_read;

impl MutationManager {
    /// Read the current summary token; a corrupt summary invalidates the guard.
    async fn check_repair_guard_token(
        &self,
        repair: &AggregateRepair,
        target_key: &KeyValue,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let status = self
            .read_summary_fields(
                repair.target_schema_name.clone(),
                target_key,
                &[AGGREGATE_GUARD_TOKEN_FIELD.to_string()],
                guard,
                storage_prefix,
                "repair",
            )
            .await?
            .unwrap_or_default();
        let current_token = status
            .get(AGGREGATE_GUARD_TOKEN_FIELD)
            .and_then(serde_json::Value::as_str);
        if current_token != repair.expected_guard_token.as_deref() {
            return Err(SchemaError::InvalidData(
                "aggregate repair guard token changed before reconciliation".into(),
            ));
        }
        Ok(())
    }

    /// Collect the member partition's reserved fields into per-key rows. Every
    /// reserved field must report the same authoritative key set.
    async fn collect_repair_member_rows(
        &self,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<HashMap<KeyValue, BTreeMap<String, serde_json::Value>>, SchemaError> {
        let mut member_schema =
            self.require_aggregate_schema("member", &guard.member_schema_name)?;
        super::super::helpers::apply_storage_prefix_to_schema(&mut member_schema, storage_prefix);
        let mut rows: HashMap<KeyValue, BTreeMap<String, serde_json::Value>> = HashMap::new();
        let mut authoritative_member_keys: Option<std::collections::HashSet<KeyValue>> = None;
        for field_name in AGGREGATE_MEMBER_RESERVED_FIELDS {
            let field = member_schema
                .runtime_fields
                .get_mut(*field_name)
                .ok_or_else(|| {
                    SchemaError::InvalidField(format!(
                        "aggregate member schema '{}' is missing reserved field '{field_name}'",
                        guard.member_schema_name
                    ))
                })?;
            let matches = field_read::collect_authoritative_hash_partition(
                field,
                &self.db_ops,
                &guard.member_partition_hash,
            )
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "aggregate member field '{field_name}' is not authoritative: {error}"
                ))
            })?;
            let raw_keys: std::collections::HashSet<KeyValue> =
                matches.iter().map(|entry| entry.key.clone()).collect();
            if let Some(expected) = &authoritative_member_keys {
                if expected != &raw_keys {
                    return Err(SchemaError::InvalidData(format!(
                        "aggregate member field '{field_name}' has a different authoritative key set"
                    )));
                }
            } else {
                authoritative_member_keys = Some(raw_keys);
            }
            for entry in matches {
                rows.entry(entry.key)
                    .or_default()
                    .insert((*field_name).to_string(), entry.value);
            }
        }
        Ok(rows)
    }

    /// Latest LWW winner per source key across the guarded source partition.
    async fn collect_repair_source_winners(
        &self,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<HashMap<KeyValue, AggregateWinner>, SchemaError> {
        let mut source_schema = self
            .schema_manager
            .get_schema_metadata(&guard.source_schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate source schema '{}' not found during repair",
                    guard.source_schema_name
                ))
            })?;
        if source_schema.schema_type != DeclarativeSchemaType::HashRange {
            return Err(SchemaError::InvalidData(
                "aggregate repair source schema must remain HashRange".into(),
            ));
        }
        super::super::helpers::apply_storage_prefix_to_schema(&mut source_schema, storage_prefix);
        let mut source_winners: HashMap<KeyValue, AggregateWinner> = HashMap::new();
        for (field_name, field) in &mut source_schema.runtime_fields {
            let matches = field_read::collect_authoritative_hash_partition(
                field,
                &self.db_ops,
                &guard.source_partition_hash,
            )
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "aggregate source field '{field_name}' is not authoritative: {error}"
                ))
            })?;
            for match_entry in matches {
                let key = match_entry.key;
                let entry = match_entry.entry;
                let winner = AggregateWinner {
                    logical_counter: entry.logical_counter,
                    written_at: entry.written_at,
                    writer_id: entry.lww_device().to_string(),
                    mutation_uuid: entry.mutation_uuid.clone(),
                };
                match source_winners.get_mut(&key) {
                    Some(current) if winner.cmp_source(current).is_gt() => *current = winner,
                    None => {
                        source_winners.insert(key, winner);
                    }
                    _ => {}
                }
            }
        }
        Ok(source_winners)
    }

    /// Write the `valid=0` summary with the repair token, then persist the
    /// durable repair grant that finalize later checks.
    async fn write_repair_summary(
        &self,
        target_schema_name: String,
        target_key: KeyValue,
        guard: &AggregateGuard,
        totals: BTreeMap<String, i64>,
        repair_token: String,
        storage_prefix: Option<&str>,
    ) -> Result<AggregateRepairReceipt, SchemaError> {
        let fields: HashMap<String, serde_json::Value> = totals
            .iter()
            .map(|(field, total)| (field.clone(), serde_json::Value::from(*total)))
            .collect();
        let summary = summary_mutation(
            target_schema_name,
            target_key,
            fields,
            false,
            repair_token.clone(),
            String::new(),
        );
        let receipt = self
            .commit_clocked_mutations(
                vec![summary],
                storage_prefix,
                WriteOrigin::Request,
                "repair summary",
                true,
            )
            .await?;
        let grant = aggregate_plan::repair_grant(repair_token.clone(), guard, totals);
        let grant_key = aggregate_plan::repair_grant_key(
            storage_prefix,
            &guard.target_schema_name,
            &guard.target_key,
        );
        self.db_ops
            .metadata()
            .put_typed_durable(&grant_key, &grant)
            .await?;
        Ok(AggregateRepairReceipt {
            receipt,
            repair_token,
        })
    }
}
