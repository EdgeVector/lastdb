//! Guard and summary decisions use captured facts, without database effects.

use super::*;

/// Guard rows after pending reservations were reconciled.
pub(super) struct GuardState {
    pub(super) current: Option<AggregateGuard>,
    pub(super) target_owner: Option<AggregateGuard>,
    pub(super) target_guard_key: String,
    pub(super) target_pending_key: String,
    pub(super) recovering_first_enrollment: bool,
}

fn different_association<'a>(
    guard: &'a Option<AggregateGuard>,
    expected: &AggregateGuard,
) -> Option<&'a AggregateGuard> {
    guard
        .as_ref()
        .filter(|current| !current.same_association(expected))
}

impl GuardState {
    pub(super) fn source_conflict(&self, expected: &AggregateGuard) -> Option<&AggregateGuard> {
        different_association(&self.current, expected)
    }

    pub(super) fn target_conflict(&self, expected: &AggregateGuard) -> Option<&AggregateGuard> {
        different_association(&self.target_owner, expected)
    }

    pub(super) fn has_association_conflict(&self, expected: &AggregateGuard) -> bool {
        self.source_conflict(expected).is_some() || self.target_conflict(expected).is_some()
    }

    pub(super) fn loses_replay_conflict(&self, expected: &AggregateGuard) -> bool {
        self.source_conflict(expected)
            .into_iter()
            .chain(self.target_conflict(expected))
            .any(|current| !expected.cmp_enrollment(current).is_gt())
    }

    pub(super) fn association_stable(&self, expected: &AggregateGuard) -> bool {
        let stable = |guard: &Option<AggregateGuard>| {
            guard
                .as_ref()
                .is_some_and(|guard| guard.committed && guard.same_association(expected))
        };
        stable(&self.current) && stable(&self.target_owner)
    }

    pub(super) fn affected<'a>(&'a self, expected: &'a AggregateGuard) -> Vec<&'a AggregateGuard> {
        let mut affected: Vec<&AggregateGuard> = self.current.iter().collect();
        affected.extend(self.target_owner.iter());
        affected.push(expected);
        affected
    }
}

/// Which guard rows this apply must publish, and which old row to drop.
pub(super) struct PublishPlan {
    pub(super) desired_guard: AggregateGuard,
    pub(super) publish_forward: bool,
    pub(super) publish_target: bool,
    pub(super) old_target_guard_key: Option<String>,
}

/// Summary row state read before the data generation.
pub(super) struct SummaryState {
    pub(super) current_summary: BTreeMap<String, i64>,
    pub(super) valid: bool,
    pub(super) guard_token: String,
}

pub(super) fn is_matching_retry(pending: &AggregateGuard, expected: &AggregateGuard) -> bool {
    pending.same_association(expected)
        && pending.cmp_enrollment(expected) == std::cmp::Ordering::Equal
}

pub(super) fn reject_request_migration(
    origin: WriteOrigin,
    migration_winner: bool,
) -> Result<(), SchemaError> {
    if matches!(origin, WriteOrigin::Request) && migration_winner {
        return Err(SchemaError::InvalidData(
            "aggregate source partition or target is already enrolled with a different association"
                .into(),
        ));
    }
    Ok(())
}

pub(super) fn plan_publish(ctx: &ApplyCtx<'_>, gs: &GuardState) -> PublishPlan {
    let expected = &ctx.expected_guard;
    let old_target_guard_key = gs.current.as_ref().and_then(|current| {
        let old_key = aggregate_target_guard_key(
            ctx.storage_prefix,
            &current.target_schema_name,
            &current.target_key,
        );
        (old_key != gs.target_guard_key).then_some(old_key)
    });
    let mut desired_guard = expected.clone();
    for existing in gs.current.iter().chain(gs.target_owner.iter()) {
        if existing.same_association(expected) && existing.cmp_enrollment(&desired_guard).is_gt() {
            desired_guard = existing.clone();
        }
    }
    let advances = |current: &AggregateGuard| {
        !current.same_association(&desired_guard) || desired_guard.cmp_enrollment(current).is_gt()
    };
    let publish_forward = gs.current.as_ref().is_none_or(advances);
    let publish_target = gs.target_owner.as_ref().is_none_or(advances);
    PublishPlan {
        desired_guard,
        publish_forward,
        publish_target,
        old_target_guard_key,
    }
}

pub(super) fn interpret_summary(
    metric_fields: &[String],
    source_uuid: &str,
    recovering_first_enrollment: bool,
    has_member: bool,
    mut summary_fields: BTreeMap<String, serde_json::Value>,
) -> Result<SummaryState, SchemaError> {
    if recovering_first_enrollment {
        summary_fields = metric_fields
            .iter()
            .cloned()
            .map(|field| (field, serde_json::Value::from(0)))
            .collect();
        summary_fields.insert(AGGREGATE_VALID_FIELD.into(), serde_json::Value::from(0));
        summary_fields.insert(
            AGGREGATE_GUARD_TOKEN_FIELD.into(),
            serde_json::Value::String(source_uuid.to_owned()),
        );
    }
    let metrics_complete = metric_fields.iter().all(|field| {
        summary_fields
            .get(field)
            .and_then(serde_json::Value::as_i64)
            .is_some()
    });
    let mut current_summary = BTreeMap::new();
    for field in metric_fields {
        current_summary.insert(
            field.clone(),
            summary_fields
                .get(field)
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
        );
    }
    let valid_value = summary_fields
        .get(AGGREGATE_VALID_FIELD)
        .and_then(serde_json::Value::as_i64);
    let old_token = summary_fields
        .get(AGGREGATE_GUARD_TOKEN_FIELD)
        .and_then(serde_json::Value::as_str);
    let status_complete = matches!(valid_value, Some(0 | 1)) && old_token.is_some();
    let summary_missing = summary_fields.is_empty();
    if !summary_missing && (!metrics_complete || !status_complete) {
        return Err(SchemaError::InvalidData(
            "aggregate summary is partial or malformed; repair is required".into(),
        ));
    }
    let valid = if summary_missing {
        false
    } else {
        valid_value == Some(1)
    };
    let mut guard_token = if summary_missing {
        source_uuid.to_owned()
    } else {
        old_token.expect("checked token").to_string()
    };
    // A valid summary without this member is a missing-member condition.
    // Keep the value repairable, but never keep it ready.
    if !has_member && !summary_fields.is_empty() && valid {
        source_uuid.clone_into(&mut guard_token);
    }
    Ok(SummaryState {
        current_summary,
        valid,
        guard_token,
    })
}
