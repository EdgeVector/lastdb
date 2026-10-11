//! Retain an exact local cut before a pre-delete cancellation resumes uploads.

use super::*;

pub(super) async fn adopt(
    host: &Host,
    engine: &SyncEngine,
    peer: &CloudPurgePeer,
    plan: &AdoptionPlan,
    control: &CloudPurgeControl,
) -> SyncResult<AdoptionReceipt> {
    let continuation = control
        .unchanged_continuation()
        .ok_or_else(|| held("cloud cancellation has no unchanged-data proof"))?;
    if control.account != plan.account
        || control.operation_id != plan.operation_id
        || control.generation != plan.generation
        || plan.continuation.as_ref() != Some(continuation)
        || fold_db::hex::sha256_hex(&serde_json::to_vec(continuation)?)
            != plan.replacement_manifest_sha256
    {
        return Err(held(
            "the local continuation differs from the cloud control",
        ));
    }
    let fence =
        match crate::cloud_purge_runtime::write_fence(host, &plan.operation_id).map_err(held)? {
            Some(fence) => fence,
            None => Arc::new(engine.acquire_cloud_purge_write_fence().await?),
        };
    let source_directory = peer
        .directory()
        .join("continuation-sources")
        .join(fold_db::hex::sha256_hex(&serde_json::to_vec(
            &plan.account,
        )?))
        .join(plan.generation.to_string())
        .join(&plan.operation_id);
    let source = engine
        .retain_cloud_purge_local_source_held(
            &source_directory,
            fold_db::sync::cloud_purge::protocol::CloudAccount {
                kind: CloudAccountKind::Personal,
                id: host.user_hash.clone(),
            },
            &fence,
        )
        .await?;
    let retained = peer.retain_directory(plan, &source_directory)?;
    peer.verify_retained_files(plan, &retained)?;
    let journal = serde_json::json!({"format_version":1,"phase":"committed","plan":plan,
        "continuation_sha256":plan.replacement_manifest_sha256,
        "source_manifest_sha256":source.prepared.manifest_sha256,
        "retention_sha256":retained.receipt_sha256,"data_and_queues_unchanged":true});
    let path = peer.directory().join(format!(
        "continuation-{}-{}-{}.json",
        fold_db::hex::sha256_hex(&serde_json::to_vec(&plan.account)?),
        plan.generation,
        plan.operation_id
    ));
    crate::cloud_purge_runtime::atomic(&path, &journal).map_err(held)?;
    Ok(AdoptionReceipt {
        account: plan.account.clone(),
        operation_id: plan.operation_id.clone(),
        generation: plan.generation,
        source_id: plan.source_id.clone(),
        retained_directory: retained.directory,
        retention_sha256: retained.receipt_sha256,
        installed_manifest_sha256: plan.replacement_manifest_sha256.clone(),
        switch_receipt_sha256: fold_db::hex::sha256_hex(&serde_json::to_vec(&journal)?),
    })
}
