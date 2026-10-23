//! Source and target ownership covers every durable repair generation.

use super::*;

impl MutationManager {
    pub(super) async fn repair_aggregate_summary_inner(
        &self,
        repair: AggregateRepair,
        access_context: &crate::access::AccessContext,
    ) -> Result<AggregateRepairReceipt, SchemaError> {
        Self::validate_repair_request(&repair)?;
        let target_key = self
            .resolve_aggregate_target_key(&repair.target_schema_name, &repair.target_key_value)?;
        let synthetic = AggregateSet {
            target_schema_name: repair.target_schema_name.clone(),
            target_key_value: target_key.clone(),
            member_schema_name: repair.member_schema_name.clone(),
            member_key_value: KeyValue::new(Some("partition".into()), Some("member".into())),
            contribution: BTreeMap::from([("placeholder".into(), 0)]),
        };
        let storage_prefix = self
            .resolve_aggregate_storage_prefix(
                &repair.source_schema_name,
                &synthetic,
                access_context,
            )
            .await?;
        let source_hash = repair
            .source_partition_key_value
            .hash
            .as_deref()
            .expect("checked source hash");
        let guard_key = aggregate_guard_key(
            storage_prefix.as_deref(),
            &repair.source_schema_name,
            source_hash,
        );
        let _guard_lock = self
            .aggregate_named_lock(format!("aggregate_guard_lock\u{1f}{guard_key}"))
            .write_owned()
            .await;
        self.with_aggregate_guards(_guard_lock, async {
            let guard = self
                .read_reconciled_aggregate_guard(&guard_key, storage_prefix.as_deref())
                .await?
                .ok_or_else(|| {
                    SchemaError::InvalidData("aggregate source partition is not enrolled".into())
                })?;
            guard.validate().map_err(SchemaError::InvalidData)?;
            Self::check_repair_guard_association(&repair, &guard, &target_key, source_hash)?;
            self.validate_enrolled_aggregate_schemas(&guard)?;

            let target_lock_key = Self::aggregate_target_lock_key(
                storage_prefix.as_deref(),
                &repair.target_schema_name,
                &target_key,
            );
            let _target_lock = self
                .aggregate_named_lock(target_lock_key)
                .write_owned()
                .await;
            self.with_aggregate_guards(_target_lock, async {
                self.repair_guarded_summary(repair, target_key, &guard, storage_prefix.as_deref())
                    .await
            })
            .await
        })
        .await
    }

    async fn repair_guarded_summary(
        &self,
        repair: AggregateRepair,
        target_key: KeyValue,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<AggregateRepairReceipt, SchemaError> {
        self.check_repair_target_owner(guard, storage_prefix)
            .await?;
        self.check_repair_guard_token(&repair, &target_key, guard, storage_prefix)
            .await?;
        let repair_token = format!("repair:{}", uuid::Uuid::new_v4());
        self.invalidate_guard_targets_durable(
            &[guard],
            &format!("repair-in-progress:{}", uuid::Uuid::new_v4()),
            "",
            storage_prefix,
            WriteOrigin::Replay,
        )
        .await?;

        let rows = self
            .collect_repair_member_rows(guard, storage_prefix)
            .await?;
        let source_winners = self
            .collect_repair_source_winners(guard, storage_prefix)
            .await?;
        let totals = self
            .reconcile_repair_members(guard, rows, &source_winners, storage_prefix)
            .await?;
        self.write_repair_summary(
            repair.target_schema_name,
            target_key,
            guard,
            totals,
            repair_token,
            storage_prefix,
        )
        .await
    }
}
