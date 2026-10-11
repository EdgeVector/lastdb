//! A cancelled first hold keeps the verified legacy descriptor and tail path.

use super::*;
use fold_db::sync::auth::AuthClient;
use fold_db::sync::cloud_purge_peer::AdoptionPlan;

pub(crate) fn bind_generation_reader(auth: &AuthClient, source: &Source) -> Phase<AuthClient> {
    let Some(observed) = &source.recovery_control else {
        return Ok(auth.clone());
    };
    let control = observed
        .control
        .as_ref()
        .ok_or_else(|| scope_failure("the recovery continuation has no control"))?;
    if control.data_generation != Some(0) || control.unchanged_continuation().is_none() {
        return Err(scope_failure(
            "legacy recovery needs a verified unchanged generation zero",
        ));
    }
    let plan = AdoptionPlan::for_readonly_control(control, "lastdb-recovery-reader")
        .map_err(|error| scope_failure(error.to_string()))?;
    Ok(auth.for_cloud_adoption_reads(&plan))
}

pub(crate) async fn reset_continuation_seed(
    apply: &Apply<'_>,
    frontier: &fold_db::sync::snapshot_log::Frontier,
) -> Phase<()> {
    if apply.source.recovery_control.is_none() {
        return Ok(());
    }
    if matches!(
        frontier,
        fold_db::sync::snapshot_log::Frontier::Scalar { .. }
    ) {
        return Err(scope_failure(
            "unchanged-cloud recovery requires the verified per-writer snapshot frontier",
        ));
    }
    let engine = &apply.engine.engine;
    engine.arm_stopped_cloud_purge_interlock().await;
    engine
        .reset_fresh_cloud_replacement_seed(&BTreeMap::from([(
            String::new(),
            frontier.as_writer_hwm(None),
        )]))
        .await
        .map_err(|error| {
            RestoreFailure::from_sync(
                Stage::RestoreTail,
                "retire inherited legacy source queues",
                &error,
            )
        })?;
    engine
        .begin_readonly_cloud_replacement_replay()
        .await
        .map_err(|error| {
            RestoreFailure::from_sync(
                Stage::RestoreTail,
                "arm unchanged-cloud read-only replay",
                &error,
            )
        })
}

pub(crate) fn verify_legacy_continuation(
    clients: &CloudClients,
    source: &Source,
) -> Phase<Option<serde_json::Value>> {
    let Some(before) = &source.recovery_control else {
        return Ok(None);
    };
    let control = before
        .control
        .as_ref()
        .ok_or_else(|| scope_failure("the recovery continuation is absent"))?;
    let after = clients
        .runtime
        .block_on(clients.auth.cloud_control_get(&control.account))
        .map_err(|error| {
            RestoreFailure::from_sync(
                Stage::SourceScope,
                "recheck unchanged cloud recovery control",
                &error,
            )
        })?;
    if after.notification_only
        || after.account != before.account
        || !after.protocol_required
        || after.generation != before.generation
        || after.control != before.control
    {
        return Err(scope_failure(
            "the unchanged cloud recovery control changed during restore",
        ));
    }
    Ok(Some(serde_json::json!({"account":control.account,
        "operation_id":control.operation_id,"cloud_generation":control.generation,
        "data_generation":0,"continuation_sha256":control.continuation_sha256,
        "remote_read_only":true,"local_adoption_acknowledged":false})))
}
