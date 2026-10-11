//! Personal accounts own their outbound share roots as well as personal data.

use super::*;

async fn rows(store: &dyn crate::storage::NamespacedStore, name: &str) -> SyncResult<Vec<Vec<u8>>> {
    let namespace = store.open_namespace(name).await?;
    let mut cursor = Vec::new();
    let mut values = Vec::new();
    loop {
        let page = namespace.scan_range_paged(&cursor, &[0xff], 128).await?;
        if page.is_empty() {
            break;
        }
        cursor = after(&page.last().expect("nonempty page").0);
        values.extend(page.into_iter().map(|(_, value)| value));
    }
    Ok(values)
}

pub(super) async fn routes(
    engine: &SyncEngine,
) -> SyncResult<(Vec<SyncTarget>, Option<SyncPartitioner>, BTreeSet<String>)> {
    let (org_rows, share_rows) = futures::join!(
        rows(engine.store.as_ref(), "org_sync_targets"),
        rows(engine.store.as_ref(), "share_rules")
    );
    let mut orgs = org_rows?
        .into_iter()
        .map(|value| serde_json::from_slice::<crate::sharing::OrgSyncTarget>(&value))
        .collect::<Result<Vec<_>, _>>()?;
    let mut shares = share_rows?
        .into_iter()
        .map(|value| serde_json::from_slice::<crate::sharing::types::ShareRule>(&value))
        .collect::<Result<Vec<_>, _>>()?;
    // Maintenance routing includes inactive streams. It cannot change their
    // saved registry flags or publish an inactive stream.
    for org in &mut orgs {
        org.active = true;
    }
    for share in &mut shares {
        share.active = true;
    }
    let mut retired = BTreeSet::from([String::new()]);
    retired.extend(shares.iter().map(|share| share.share_prefix.clone()));
    if orgs.iter().any(|org| retired.contains(&org.org_hash)) {
        return Err(SyncError::CloudPurgeHeld(
            "an owned share root collides with an org cloud account".into(),
        ));
    }
    let mut targets = engine.targets.lock().await.clone();
    for prefix in orgs
        .iter()
        .map(|org| &org.org_hash)
        .chain(shares.iter().map(|share| &share.share_prefix))
    {
        if !targets.iter().any(|target| target.prefix == *prefix) {
            targets.push(SyncTarget {
                label: "stopped queue routing".into(),
                prefix: prefix.clone(),
                crypto: engine.crypto.clone(),
            });
        }
    }
    Ok((
        targets,
        Some(SyncPartitioner::new_with_orgs(&shares, &orgs)),
        retired,
    ))
}
