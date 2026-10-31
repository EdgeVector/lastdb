//! Aggregate guard invalidation and reconciliation.

use super::*;

impl MutationManager {
    /// Persist invalid readiness before an association guard can move.
    pub(super) async fn invalidate_guard_targets_durable(
        &self,
        guards: &[&AggregateGuard],
        token: &str,
        pub_key: &str,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
    ) -> Result<(), SchemaError> {
        let invalidations = self
            .build_guard_target_invalidations(guards, token, pub_key, storage_prefix)
            .await?;
        if invalidations.is_empty() {
            return Ok(());
        }
        self.commit_clocked_mutations(invalidations, storage_prefix, origin, "invalidation", false)
            .await?;
        self.verify_guard_invalidation_markers(guards, token, storage_prefix)
            .await?;
        self.delete_guard_grants(guards, storage_prefix).await?;
        Ok(())
    }

    /// Build one invalid-readiness mutation per distinct aggregate target.
    async fn build_guard_target_invalidations(
        &self,
        guards: &[&AggregateGuard],
        token: &str,
        pub_key: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Mutation>, SchemaError> {
        let targets = invalidation_rules::guard_targets(guards);
        let mut invalidations = Vec::with_capacity(targets.len());
        for ((target_schema_name, target_key), metric_fields) in targets {
            let target_schema = self
                .schema_manager
                .get_schema_metadata(&target_schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "aggregate target schema '{target_schema_name}' not found during guard migration"
                    ))
                })?;
            invalidation_rules::check_target_fields(&target_schema, &target_schema_name)?;
            let target_key_value = KeyValue::from_storage_key(&target_key)?;
            let metric_names: Vec<String> = metric_fields.into_iter().collect();
            let probe = Self::aggregate_probe(target_schema_name.clone(), target_key_value.clone());
            let current = self
                .read_current_row_fields(&probe, &metric_names, storage_prefix)
                .await?;
            let values = invalidation_rules::missing_metric_values(&metric_names, &current);
            invalidations.push(super::rules::summary_mutation(
                target_schema_name,
                target_key_value,
                values,
                false,
                token.to_string(),
                pub_key.to_string(),
            ));
        }
        Ok(invalidations)
    }

    /// Read back each target and confirm the invalidation marker is current.
    async fn verify_guard_invalidation_markers(
        &self,
        guards: &[&AggregateGuard],
        token: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let mut verified_targets = std::collections::BTreeSet::new();
        for guard in guards {
            if !verified_targets
                .insert((guard.target_schema_name.clone(), guard.target_key.clone()))
            {
                continue;
            }
            let probe = Self::aggregate_probe(
                guard.target_schema_name.clone(),
                KeyValue::from_storage_key(&guard.target_key)?,
            );
            let status = self
                .read_current_row_fields(
                    &probe,
                    &[
                        AGGREGATE_VALID_FIELD.to_string(),
                        AGGREGATE_GUARD_TOKEN_FIELD.to_string(),
                    ],
                    storage_prefix,
                )
                .await?;
            invalidation_rules::check_marker(&status, token)?;
        }
        Ok(())
    }

    /// Delete the repair and finalized grants of every invalidated target.
    async fn delete_guard_grants(
        &self,
        guards: &[&AggregateGuard],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        for guard in guards {
            self.delete_aggregate_target_grants(
                storage_prefix,
                &guard.target_schema_name,
                &guard.target_key,
            )
            .await?;
        }
        Ok(())
    }

    /// Discard a crash-left provisional guard and fail its summary closed.
    /// Different schema lanes can persist different parts of a resident batch
    /// before a crash, so no single derived row can promote the reservation.
    pub(super) async fn reconcile_aggregate_guard(
        &self,
        key: &str,
        guard: AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<Option<AggregateGuard>, SchemaError> {
        guard.validate().map_err(SchemaError::InvalidData)?;
        if guard.committed {
            return Ok(Some(guard));
        }
        self.invalidate_guard_targets_durable(
            &[&guard],
            &format!("guard-recovery:{}", uuid::Uuid::new_v4()),
            "",
            storage_prefix,
            WriteOrigin::Replay,
        )
        .await?;
        self.db_ops.metadata().delete_typed_durable(key).await?;
        Ok(None)
    }

    pub(super) async fn read_reconciled_aggregate_guard(
        &self,
        key: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<AggregateGuard>, SchemaError> {
        let Some(guard) = self
            .db_ops
            .metadata()
            .get_typed::<AggregateGuard>(key)
            .await?
        else {
            return Ok(None);
        };
        self.reconcile_aggregate_guard(key, guard, storage_prefix)
            .await
    }

    /// The committed target owner must be this guard. `mismatch` is the refusal
    /// text when a different source partition owns the target.
    pub(super) async fn check_aggregate_target_owner(
        &self,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
        mismatch: &str,
    ) -> Result<(), SchemaError> {
        let reverse_key = aggregate_target_guard_key(
            storage_prefix,
            &guard.target_schema_name,
            &guard.target_key,
        );
        let reverse = self
            .read_active_target_guard(&reverse_key, storage_prefix)
            .await?
            .ok_or_else(|| {
                SchemaError::InvalidData("aggregate target has no committed owner".into())
            })?;
        if !aggregate_plan::target_owner_matches(&reverse, guard) {
            return Err(SchemaError::InvalidData(mismatch.into()));
        }
        Ok(())
    }

    /// Read a reverse target owner and discard an owner whose forward guard
    /// moved elsewhere before a crash.
    pub(super) async fn read_active_target_guard(
        &self,
        target_guard_key: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<AggregateGuard>, SchemaError> {
        let Some(owner) = self
            .read_reconciled_aggregate_guard(target_guard_key, storage_prefix)
            .await?
        else {
            return Ok(None);
        };
        let forward_key = aggregate_guard_key(
            storage_prefix,
            &owner.source_schema_name,
            &owner.source_partition_hash,
        );
        let forward = self
            .read_reconciled_aggregate_guard(&forward_key, storage_prefix)
            .await?;
        if let Some(forward) = forward.filter(|forward| forward.same_association(&owner)) {
            if forward.cmp_enrollment(&owner).is_gt() {
                self.db_ops
                    .metadata()
                    .put_typed_durable(target_guard_key, &forward)
                    .await?;
            }
            Ok(Some(forward))
        } else {
            self.db_ops
                .metadata()
                .delete_typed_durable(target_guard_key)
                .await?;
            Ok(None)
        }
    }

    /// Read summary fields for `kind`. `None` means the row is absent.
    /// A corrupt field invalidates the guard, then returns the same refusal.
    pub(super) async fn read_summary_fields(
        &self,
        schema_name: String,
        target_key: &KeyValue,
        field_names: &[String],
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
        kind: &str,
    ) -> Result<Option<BTreeMap<String, serde_json::Value>>, SchemaError> {
        let probe = Self::aggregate_probe(schema_name, target_key.clone());
        match self
            .read_current_row_fields(&probe, field_names, storage_prefix)
            .await?
        {
            CurrentRowFields::Absent => Ok(None),
            CurrentRowFields::Present(fields) => Ok(Some(fields)),
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                self.invalidate_guard_targets_durable(
                    &[guard],
                    &format!("{kind}-corrupt:{}", uuid::Uuid::new_v4()),
                    "",
                    storage_prefix,
                    WriteOrigin::Replay,
                )
                .await?;
                Err(SchemaError::InvalidData(format!(
                    "aggregate {kind} found unresolved summary field '{field}' atom '{atom_uuid}'"
                )))
            }
        }
    }
}
