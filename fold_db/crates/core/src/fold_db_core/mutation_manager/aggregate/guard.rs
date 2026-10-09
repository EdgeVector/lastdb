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
        // lint:fn-size-ok moved verbatim from the original impl; splitting is a separate change
        let mut targets: BTreeMap<(String, String), std::collections::BTreeSet<String>> =
            BTreeMap::new();
        for guard in guards {
            targets
                .entry((guard.target_schema_name.clone(), guard.target_key.clone()))
                .or_default()
                .extend(guard.metric_fields.iter().cloned());
        }
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
            for field in [AGGREGATE_VALID_FIELD, AGGREGATE_GUARD_TOKEN_FIELD] {
                if !target_schema.runtime_fields.contains_key(field) {
                    return Err(SchemaError::InvalidField(format!(
                        "aggregate target schema '{target_schema_name}' must declare field '{field}'"
                    )));
                }
            }
            let target_key_value = KeyValue::from_storage_key(&target_key);
            let metric_names: Vec<String> = metric_fields.into_iter().collect();
            let probe = Self::aggregate_probe(target_schema_name.clone(), target_key_value.clone());
            let current = self
                .read_current_row_fields(&probe, &metric_names, storage_prefix)
                .await?;
            let mut values = HashMap::new();
            match current {
                CurrentRowFields::Absent | CurrentRowFields::Corrupt { .. } => {
                    values.extend(
                        metric_names
                            .iter()
                            .cloned()
                            .map(|field| (field, serde_json::Value::from(0))),
                    );
                }
                CurrentRowFields::Present(current) => {
                    values.extend(
                        metric_names
                            .iter()
                            .filter(|field| {
                                current
                                    .get(*field)
                                    .and_then(serde_json::Value::as_i64)
                                    .is_none()
                            })
                            .map(|field| (field.clone(), serde_json::Value::from(0))),
                    );
                }
            }
            values.insert(AGGREGATE_VALID_FIELD.into(), serde_json::Value::from(0));
            values.insert(
                AGGREGATE_GUARD_TOKEN_FIELD.into(),
                serde_json::Value::String(token.to_string()),
            );
            let mut invalidation = Mutation::new(
                target_schema_name,
                values,
                target_key_value,
                pub_key.to_string(),
                MutationType::Update,
            );
            invalidation.aggregate_derived_internal = true;
            invalidation.synchronous = Some(true);
            invalidations.push(invalidation);
        }
        if invalidations.is_empty() {
            return Ok(());
        }
        let clock = self.prepare_mutation_author_clocks(&mut invalidations)?;
        if let Some((reservation, state)) = clock {
            reservation.submit_and_wait(state).await?;
        }
        let result = self
            .write_mutations_batch_inner(invalidations, storage_prefix, origin)
            .await;
        let receipt = Self::require_durable_receipt(result?, "invalidation")?;
        let _ = receipt;
        let mut verified_targets = std::collections::BTreeSet::new();
        for guard in guards {
            if !verified_targets
                .insert((guard.target_schema_name.clone(), guard.target_key.clone()))
            {
                continue;
            }
            let probe = Self::aggregate_probe(
                guard.target_schema_name.clone(),
                KeyValue::from_storage_key(&guard.target_key),
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
            let CurrentRowFields::Present(status) = status else {
                return Err(SchemaError::InvalidData(
                    "aggregate invalidation marker did not become readable".into(),
                ));
            };
            if status
                .get(AGGREGATE_VALID_FIELD)
                .and_then(serde_json::Value::as_i64)
                != Some(0)
                || status
                    .get(AGGREGATE_GUARD_TOKEN_FIELD)
                    .and_then(serde_json::Value::as_str)
                    != Some(token)
            {
                return Err(SchemaError::InvalidData(
                    "aggregate invalidation marker did not become current".into(),
                ));
            }
        }
        for guard in guards {
            let grant_key = Self::aggregate_repair_grant_key(
                storage_prefix,
                &guard.target_schema_name,
                &guard.target_key,
            );
            self.db_ops
                .metadata()
                .delete_typed_durable(&grant_key)
                .await?;
            let finalized_key = Self::aggregate_finalized_grant_key(
                storage_prefix,
                &guard.target_schema_name,
                &guard.target_key,
            );
            self.db_ops
                .metadata()
                .delete_typed_durable(&finalized_key)
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
}
