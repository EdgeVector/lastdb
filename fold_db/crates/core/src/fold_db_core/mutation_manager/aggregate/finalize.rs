//! Aggregate summary finalize path.

use super::*;

impl MutationManager {
    /// Mark one independently repaired summary valid if no source write raced.
    pub async fn finalize_aggregate_summary_with_access_receipt(
        &self,
        finalize: AggregateFinalize,
        access_context: &crate::access::AccessContext,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        #[cfg(feature = "cloud-sync")]
        {
            return crate::sync::capture::with_capture_suppressed(
                self.finalize_aggregate_summary_inner(finalize, access_context),
            )
            .await;
        }
        #[cfg(not(feature = "cloud-sync"))]
        self.finalize_aggregate_summary_inner(finalize, access_context)
            .await
    }

    pub(super) async fn finalize_aggregate_summary_inner(
        &self,
        finalize: AggregateFinalize,
        access_context: &crate::access::AccessContext,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        // lint:fn-size-ok moved verbatim from the original impl; splitting is a separate change
        if finalize.source_schema_name.trim().is_empty()
            || finalize.target_schema_name.trim().is_empty()
            || finalize.member_schema_name.trim().is_empty()
            || finalize.expected_guard_token.trim().is_empty()
        {
            return Err(SchemaError::InvalidData(
                "aggregate finalize identities and expected_guard_token must not be empty".into(),
            ));
        }
        if finalize.source_partition_key_value.hash.is_none()
            || finalize.source_partition_key_value.range.is_some()
        {
            return Err(SchemaError::InvalidData(
                "aggregate finalize source_partition_key_value must be hash-only".into(),
            ));
        }
        let target_key = self.resolve_aggregate_target_key(
            &finalize.target_schema_name,
            &finalize.target_key_value,
        )?;
        let synthetic = AggregateSet {
            target_schema_name: finalize.target_schema_name.clone(),
            target_key_value: target_key.clone(),
            member_schema_name: finalize.member_schema_name.clone(),
            member_key_value: KeyValue::new(Some("partition".into()), Some("member".into())),
            contribution: BTreeMap::from([("placeholder".into(), 0)]),
        };
        let storage_prefix = self
            .resolve_aggregate_storage_prefix(
                &finalize.source_schema_name,
                &synthetic,
                access_context,
            )
            .await?;
        let source_hash = finalize
            .source_partition_key_value
            .hash
            .as_deref()
            .expect("checked hash");
        let guard_key = aggregate_guard_key(
            storage_prefix.as_deref(),
            &finalize.source_schema_name,
            source_hash,
        );
        let _guard_lock = self
            .aggregate_named_lock(format!("aggregate_guard_lock\u{1f}{guard_key}"))
            .lock_owned()
            .await;
        let guard = self
            .read_reconciled_aggregate_guard(&guard_key, storage_prefix.as_deref())
            .await?
            .ok_or_else(|| {
                SchemaError::InvalidData("aggregate source partition is not enrolled".into())
            })?;
        guard.validate().map_err(SchemaError::InvalidData)?;
        if guard.source_schema_name != finalize.source_schema_name
            || guard.source_partition_hash != source_hash
            || guard.target_schema_name != finalize.target_schema_name
            || guard.target_key != target_key.to_storage_key()
            || guard.member_schema_name != finalize.member_schema_name
            || !guard.committed
        {
            return Err(SchemaError::InvalidData(
                "aggregate finalize association does not match the enrolled guard".into(),
            ));
        }
        self.validate_enrolled_aggregate_schemas(&guard)?;

        let target_lock_key = Self::aggregate_target_lock_key(
            storage_prefix.as_deref(),
            &finalize.target_schema_name,
            &target_key,
        );
        let _target_lock = self
            .aggregate_named_lock(target_lock_key)
            .lock_owned()
            .await;
        let reverse_key = aggregate_target_guard_key(
            storage_prefix.as_deref(),
            &guard.target_schema_name,
            &guard.target_key,
        );
        let reverse = self
            .read_active_target_guard(&reverse_key, storage_prefix.as_deref())
            .await?
            .ok_or_else(|| {
                SchemaError::InvalidData("aggregate target has no committed owner".into())
            })?;
        if !reverse.same_association(&guard)
            || reverse.cmp_enrollment(&guard) != std::cmp::Ordering::Equal
        {
            return Err(SchemaError::InvalidData(
                "aggregate target owner does not match the finalize source partition".into(),
            ));
        }
        let grant_key = Self::aggregate_repair_grant_key(
            storage_prefix.as_deref(),
            &guard.target_schema_name,
            &guard.target_key,
        );
        let finalized_key = Self::aggregate_finalized_grant_key(
            storage_prefix.as_deref(),
            &guard.target_schema_name,
            &guard.target_key,
        );
        let active_grant = self
            .db_ops
            .metadata()
            .get_typed::<AggregateRepairGrant>(&grant_key)
            .await?;
        let finalized_grant = self
            .db_ops
            .metadata()
            .get_typed::<AggregateRepairGrant>(&finalized_key)
            .await?;
        let grant = active_grant
            .as_ref()
            .or(finalized_grant.as_ref())
            .ok_or_else(|| {
                SchemaError::InvalidData(
                    "aggregate finalize requires a durable repair grant".into(),
                )
            })?;
        let probe = Self::aggregate_probe(finalize.target_schema_name.clone(), target_key.clone());
        let mut summary_field_names = guard.metric_fields.clone();
        summary_field_names.push(AGGREGATE_VALID_FIELD.to_string());
        summary_field_names.push(AGGREGATE_GUARD_TOKEN_FIELD.to_string());
        let status = match self
            .read_current_row_fields(&probe, &summary_field_names, storage_prefix.as_deref())
            .await?
        {
            CurrentRowFields::Absent => {
                return Err(SchemaError::InvalidData(
                    "aggregate finalize summary is absent".into(),
                ));
            }
            CurrentRowFields::Present(fields) => fields,
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                self.invalidate_guard_targets_durable(
                    &[&guard],
                    &format!("finalize-corrupt:{}", uuid::Uuid::new_v4()),
                    "",
                    storage_prefix.as_deref(),
                    WriteOrigin::Replay,
                )
                .await?;
                return Err(SchemaError::InvalidData(format!(
                    "aggregate finalize found unresolved summary field '{field}' atom '{atom_uuid}'"
                )));
            }
        };
        let current_token = status
            .get(AGGREGATE_GUARD_TOKEN_FIELD)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                SchemaError::InvalidData("aggregate summary has no guard token".into())
            })?;
        if current_token != finalize.expected_guard_token {
            return Err(SchemaError::InvalidData(
                "aggregate finalize guard token changed during repair".into(),
            ));
        }
        let mut totals = BTreeMap::new();
        for field in &guard.metric_fields {
            let value = status
                .get(field)
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "aggregate finalize summary metric '{field}' is missing or malformed"
                    ))
                })?;
            totals.insert(field.clone(), value);
        }
        if grant.version != 1
            || grant.repair_token != finalize.expected_guard_token
            || grant.association_fingerprint != guard.association_fingerprint
            || grant.enrollment_winner != guard.enrollment_winner
            || grant.totals != totals
            || grant.totals_fingerprint != Self::aggregate_totals_fingerprint(&totals)
        {
            return Err(SchemaError::InvalidData(
                "aggregate finalize repair grant does not match the complete metric generation"
                    .into(),
            ));
        }
        let valid = status
            .get(AGGREGATE_VALID_FIELD)
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| {
                SchemaError::InvalidData("aggregate summary has no integer valid marker".into())
            })?;
        if !matches!(valid, 0 | 1) {
            return Err(SchemaError::InvalidData(
                "aggregate summary valid marker must be 0 or 1".into(),
            ));
        }
        if valid == 1 {
            if active_grant.is_some() {
                self.db_ops
                    .metadata()
                    .put_typed_durable(&finalized_key, grant)
                    .await?;
                self.db_ops
                    .metadata()
                    .delete_typed_durable(&grant_key)
                    .await?;
            }
            let mut receipt = ResidentCommitReceipt::empty();
            receipt.durability = ResidentDurability::Durable;
            return Ok(receipt);
        }
        if active_grant.is_none() {
            return Err(SchemaError::InvalidData(
                "aggregate finalized proof cannot authorize a new valid marker".into(),
            ));
        }

        let mut summary = Mutation::new(
            finalize.target_schema_name,
            HashMap::from([
                (AGGREGATE_VALID_FIELD.into(), serde_json::Value::from(1)),
                (
                    AGGREGATE_GUARD_TOKEN_FIELD.into(),
                    serde_json::Value::String(finalize.expected_guard_token),
                ),
            ]),
            target_key,
            String::new(),
            MutationType::Update,
        );
        summary.aggregate_derived_internal = true;
        summary.synchronous = Some(true);
        let clock = self.prepare_mutation_author_clocks(std::slice::from_mut(&mut summary))?;
        let revision = summary.logical_counter;
        if let Some((reservation, state)) = clock {
            reservation.submit_and_wait(state).await?;
        }
        #[cfg(feature = "cloud-sync")]
        let result =
            crate::sync::capture::with_capture_suppressed(self.write_mutations_batch_inner(
                vec![summary],
                storage_prefix.as_deref(),
                WriteOrigin::Request,
            ))
            .await;
        #[cfg(not(feature = "cloud-sync"))]
        let result = self
            .write_mutations_batch_inner(
                vec![summary],
                storage_prefix.as_deref(),
                WriteOrigin::Request,
            )
            .await;
        let mut receipt = Self::require_durable_receipt(result?, "finalize marker")?;
        receipt.revision = Some(revision);
        self.db_ops
            .metadata()
            .put_typed_durable(&finalized_key, grant)
            .await?;
        self.db_ops
            .metadata()
            .delete_typed_durable(&grant_key)
            .await?;
        Ok(receipt)
    }
}
