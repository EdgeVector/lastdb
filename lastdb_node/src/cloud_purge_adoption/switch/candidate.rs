//! Build a fresh inactive candidate before any serving directory changes.

use super::*;

pub(super) async fn prepare(
    home: &Path,
    pending: &Pending,
    retained: &fold_db::sync::cloud_purge_peer::RetainedHome,
) -> Result<(PathBuf, String), String> {
    let candidate = pending.staged_home.with_file_name(format!(
        "{}-candidate-{}",
        pending.plan.operation_id,
        uuid::Uuid::new_v4()
    ));
    if pending.plan.account.kind == CloudAccountKind::Personal {
        files::copy_data(&pending.staged_home.join("data"), &candidate.join("data"))?;
        files::copy_identity(home, &candidate)?;
        files::copy_root_sidecars(&pending.staged_home, &candidate)?;
        crate::cloud_purge_adoption_org::preserve_unrelated_orgs(home, &candidate).await?;
        let replacement: CloudReplacementManifest =
            serde_json::from_value(pending.plan.replacement_manifest.clone()).map_err(message)?;
        let root = local_store_root(&replacement).map_err(message)?;
        let digest = crate::cloud_purge_adoption_org::reset_personal_candidate_generation(
            home,
            &candidate,
            &pending.plan,
            retained,
            &root.writer_frontier,
        )
        .await?;
        return Ok((candidate, digest));
    }
    files::copy_data(&retained.directory.join("home"), &candidate.join("data"))?;
    files::copy_identity(home, &candidate)?;
    // An org merge retains the personal physical store and its high-water identity.
    files::copy_root_sidecars(home, &candidate)?;
    let keys = crate::cloud_purge_adoption_org::identity_keys(home)?;
    let (_, ops) =
        crate::cloud_purge_adoption_org::open_local_store(home, keys.encryption_key()).await?;
    let registry = ops
        .open_namespace("org_sync_targets")
        .await
        .map_err(message)?;
    let target: fold_db::sharing::org_sync_target::OrgSyncTarget = serde_json::from_slice(
        &registry
            .get(format!("org_sync:{}", pending.plan.account.id).as_bytes())
            .await
            .map_err(message)?
            .ok_or("the peer org sync target disappeared")?,
    )
    .map_err(message)?;
    let key = crate::cloud_purge_adoption_org::org_key(&target)?;
    let replacement: CloudReplacementManifest =
        serde_json::from_value(pending.plan.replacement_manifest.clone()).map_err(message)?;
    let root = local_store_root(&replacement).map_err(message)?;
    let catalog =
        crate::cloud_purge_adoption_org::read_org_catalog(root, &pending.staged_home, &key).await?;
    crate::cloud_purge_adoption_org::merge_org_replacement(
        home,
        &pending.staged_home,
        &candidate,
        &catalog,
    )
    .await?;
    let digest = crate::cloud_purge_adoption_org::reset_org_candidate_generation(
        home,
        &candidate,
        &pending.plan,
        retained,
        &root.writer_frontier,
    )
    .await?;
    Ok((candidate, digest))
}
