//! Aggregate summary bounded-repair path.
// lint:file-size-ok moved verbatim from the original file; one method family per file

use super::*;

impl MutationManager {
    /// Recompute a full summary from the bounded member partition.
    ///
    /// The target lock covers the member range read and summary write. A
    /// concurrent aggregate mutation therefore lands wholly before or after
    /// this repair. The repair keeps `valid=0`; callers finalize separately
    /// after their independent source/member reconciliation.
    pub async fn repair_aggregate_summary_with_access_receipt(
        &self,
        repair: AggregateRepair,
        access_context: &crate::access::AccessContext,
    ) -> Result<AggregateRepairReceipt, SchemaError> {
        #[cfg(feature = "cloud-sync")]
        {
            return crate::sync::capture::with_capture_suppressed(
                self.repair_aggregate_summary_inner(repair, access_context),
            )
            .await;
        }
        #[cfg(not(feature = "cloud-sync"))]
        self.repair_aggregate_summary_inner(repair, access_context)
            .await
    }

    pub(super) async fn repair_aggregate_summary_inner(
        &self,
        repair: AggregateRepair,
        access_context: &crate::access::AccessContext,
    ) -> Result<AggregateRepairReceipt, SchemaError> {
        // lint:fn-size-ok moved verbatim from the original impl; splitting is a separate change
        if repair.source_partition_key_value.hash.is_none()
            || repair.source_partition_key_value.range.is_some()
        {
            return Err(SchemaError::InvalidData(
                "aggregate repair requires a hash-only source partition".into(),
            ));
        }
        if repair
            .expected_guard_token
            .as_deref()
            .is_some_and(|token| token.trim().is_empty())
        {
            return Err(SchemaError::InvalidData(
                "aggregate repair expected_guard_token must not be empty".into(),
            ));
        }
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
            .lock_owned()
            .await;
        let guard = self
            .read_reconciled_aggregate_guard(&guard_key, storage_prefix.as_deref())
            .await?
            .ok_or_else(|| {
                SchemaError::InvalidData("aggregate source partition is not enrolled".into())
            })?;
        guard.validate().map_err(SchemaError::InvalidData)?;
        if guard.source_schema_name != repair.source_schema_name
            || guard.source_partition_hash != source_hash
            || guard.target_schema_name != repair.target_schema_name
            || guard.target_key != target_key.to_storage_key()
            || guard.member_schema_name != repair.member_schema_name
            || !guard.committed
        {
            return Err(SchemaError::InvalidData(
                "aggregate repair association does not match the enrolled guard".into(),
            ));
        }
        self.validate_enrolled_aggregate_schemas(&guard)?;

        let target_lock_key = Self::aggregate_target_lock_key(
            storage_prefix.as_deref(),
            &repair.target_schema_name,
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
                "aggregate target owner does not match the repair source partition".into(),
            ));
        }
        let summary_probe =
            Self::aggregate_probe(repair.target_schema_name.clone(), target_key.clone());
        let status = match self
            .read_current_row_fields(
                &summary_probe,
                &[AGGREGATE_GUARD_TOKEN_FIELD.to_string()],
                storage_prefix.as_deref(),
            )
            .await?
        {
            CurrentRowFields::Absent => BTreeMap::new(),
            CurrentRowFields::Present(fields) => fields,
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                self.invalidate_guard_targets_durable(
                    &[&guard],
                    &format!("repair-corrupt:{}", uuid::Uuid::new_v4()),
                    "",
                    storage_prefix.as_deref(),
                    WriteOrigin::Replay,
                )
                .await?;
                return Err(SchemaError::InvalidData(format!(
                    "aggregate repair found unresolved summary field '{field}' atom '{atom_uuid}'"
                )));
            }
        };
        let current_token = status
            .get(AGGREGATE_GUARD_TOKEN_FIELD)
            .and_then(serde_json::Value::as_str);
        if current_token != repair.expected_guard_token.as_deref() {
            return Err(SchemaError::InvalidData(
                "aggregate repair guard token changed before reconciliation".into(),
            ));
        }
        let repair_token = format!("repair:{}", uuid::Uuid::new_v4());
        self.invalidate_guard_targets_durable(
            &[&guard],
            &format!("repair-in-progress:{}", uuid::Uuid::new_v4()),
            "",
            storage_prefix.as_deref(),
            WriteOrigin::Replay,
        )
        .await?;

        let mut member_schema = self
            .schema_manager
            .get_schema_metadata(&guard.member_schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate member schema '{}' not found",
                    guard.member_schema_name
                ))
            })?;
        super::super::helpers::apply_storage_prefix_to_schema(
            &mut member_schema,
            storage_prefix.as_deref(),
        );
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
            let matches = field
                .collect_authoritative_hash_partition(&self.db_ops, &guard.member_partition_hash)
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

        let mut totals: BTreeMap<String, i64> = guard
            .metric_fields
            .iter()
            .cloned()
            .map(|field| (field, 0))
            .collect();
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
        super::super::helpers::apply_storage_prefix_to_schema(
            &mut source_schema,
            storage_prefix.as_deref(),
        );
        let source_schema_type = source_schema.schema_type;
        let mut source_winners: HashMap<KeyValue, AggregateWinner> = HashMap::new();
        for (field_name, field) in &mut source_schema.runtime_fields {
            let matches = field
                .collect_authoritative_hash_partition(&self.db_ops, &guard.source_partition_hash)
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
        let mut source_members = std::collections::HashSet::new();
        let mut absent_member_purges = Vec::new();
        for (member_key, fields) in rows {
            if fields.len() != AGGREGATE_MEMBER_RESERVED_FIELDS.len() {
                return Err(SchemaError::InvalidData(format!(
                    "aggregate member '{}' is partial",
                    member_key.to_storage_key()
                )));
            }
            if member_key.hash.as_deref() != Some(guard.member_partition_hash.as_str()) {
                return Err(SchemaError::InvalidData(
                    "aggregate repair returned a member outside its guarded partition".into(),
                ));
            }
            let member =
                AggregateMemberRecord::from_fields(&fields).map_err(SchemaError::InvalidData)?;
            let member_source_key = KeyValue::from_storage_key(&member.source_key);
            let source_key_shape_matches = source_schema_type == DeclarativeSchemaType::HashRange
                && member_source_key.range.is_some();
            let canonical_member_key = member_source_key.clone();
            if member.source_schema_name != guard.source_schema_name
                || member_source_key.hash.as_deref() != Some(guard.source_partition_hash.as_str())
                || !source_key_shape_matches
                || member_key != canonical_member_key
                || member.target_schema_name != guard.target_schema_name
                || member.target_key != guard.target_key
                || member.metric_fingerprint != guard.metric_fingerprint
                || member.contribution.keys().cloned().collect::<Vec<_>>() != guard.metric_fields
                || member.contribution.values().any(|value| *value < 0)
            {
                return Err(SchemaError::InvalidData(
                    "aggregate repair found a member outside the guarded association".into(),
                ));
            }
            let Some(source_winner) = source_winners.get(&member_source_key) else {
                let mut purge = Mutation::new(
                    guard.member_schema_name.clone(),
                    HashMap::new(),
                    member_key,
                    String::new(),
                    MutationType::Purge,
                );
                purge.aggregate_derived_internal = true;
                absent_member_purges.push(purge);
                continue;
            };
            if source_winner != &member.winner {
                return Err(SchemaError::InvalidData(format!(
                    "aggregate member '{}' does not match the latest source winner",
                    member_source_key.to_storage_key()
                )));
            }
            if !source_members.insert(member_source_key) {
                return Err(SchemaError::InvalidData(
                    "aggregate repair found duplicate rows for one source member".into(),
                ));
            }
            for (field, contribution) in member.contribution {
                let total = totals
                    .get_mut(&field)
                    .expect("member metric set matched guard");
                *total = total.checked_add(contribution).ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "aggregate repair overflow for metric '{field}'"
                    ))
                })?;
            }
        }

        if storage_prefix.is_some() && !absent_member_purges.is_empty() {
            return Err(SchemaError::InvalidData(
                "scoped aggregate repair found orphan members; scoped hard cleanup is not supported"
                    .into(),
            ));
        }
        if !absent_member_purges.is_empty() {
            self.enqueue_hard_erasures_on_lanes(
                absent_member_purges,
                storage_prefix.as_deref(),
                crate::fold_db_core::purge::PurgeMissingPolicy::Skip,
                crate::fold_db_core::purge::HardEraseVerb::Purge,
                true,
                None,
            )
            .await?;
        }
        if source_members.len() != source_winners.len()
            || source_winners
                .keys()
                .any(|source_key| !source_members.contains(source_key))
        {
            return Err(SchemaError::InvalidData(
                "aggregate repair source/member key sets are not a bijection".into(),
            ));
        }

        let mut fields: HashMap<String, serde_json::Value> = totals
            .iter()
            .map(|(field, total)| (field.clone(), serde_json::Value::from(*total)))
            .collect();
        fields.insert(AGGREGATE_VALID_FIELD.into(), serde_json::Value::from(0));
        fields.insert(
            AGGREGATE_GUARD_TOKEN_FIELD.into(),
            serde_json::Value::String(repair_token.clone()),
        );
        let mut summary = Mutation::new(
            repair.target_schema_name,
            fields,
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
        let mut receipt = Self::require_durable_receipt(result?, "repair summary")?;
        receipt.revision = Some(revision);
        let grant = AggregateRepairGrant {
            version: 1,
            repair_token: repair_token.clone(),
            association_fingerprint: guard.association_fingerprint.clone(),
            enrollment_winner: guard.enrollment_winner.clone(),
            totals_fingerprint: Self::aggregate_totals_fingerprint(&totals),
            totals,
        };
        let grant_key = Self::aggregate_repair_grant_key(
            storage_prefix.as_deref(),
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
