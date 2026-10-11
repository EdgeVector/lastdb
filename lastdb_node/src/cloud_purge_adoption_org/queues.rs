//! Queue retirement runs against the inactive copied org candidate only.

use super::*;
use fold_db::sync::auth::{AuthClient, SyncAuth};
use fold_db::sync::cloud_purge_peer::{AdoptionPlan, CloudPurgePeer, RetainedHome};
use fold_db::sync::{SyncConfig, SyncEngine, SyncPartitioner, SyncTarget};
use std::collections::BTreeMap;

/// Retire the old org stream using the old routing, after the new scoped data
/// merge. No network request, runtime worker, or catalog mutation occurs here.
pub(crate) async fn reset_org_candidate_generation(
    old_stopped_home: &Path,
    candidate_home: &Path,
    plan: &AdoptionPlan,
    retained: &RetainedHome,
    replacement_frontier: &BTreeMap<String, u64>,
) -> Result<String, String> {
    if plan.account.kind != fold_db::sync::cloud_purge::protocol::CloudAccountKind::Org {
        return Err("an org queue reset requires an org account".into());
    }
    let keys = identity_keys(old_stopped_home)?;
    let (_, prior) = open_peer_store(old_stopped_home, &keys).await?;
    let (raw, candidate) = open_peer_store(candidate_home, &keys).await?;
    let orgs = fold_db::sharing::list_active_org_sync_targets_in_ops(&prior)
        .await
        .map_err(message)?;
    let shares = fold_db::sharing::store::list_share_rules_in_ops(&prior)
        .await
        .map_err(message)?;
    if !orgs.iter().any(|target| target.org_hash == plan.account.id) {
        return Err("the prior org routing target is absent".into());
    }
    let engine = stopped_engine(
        old_stopped_home,
        plan,
        &keys,
        &candidate,
        raw.clone(),
        &orgs,
        &shares,
    )
    .await?;
    let receipt = engine
        .detach_cloud_purge_queues(plan, retained, &plan.account.id, replacement_frontier)
        .await
        .map_err(message)?;
    candidate
        .namespaced_store()
        .restore_durability_barrier()
        .await
        .map_err(message)?;
    raw.verify_integrity().map_err(message)?;
    receipt.sha256().map_err(message)
}

/// Reset only the adopted personal stream, and preserve the stopped peer's
/// unrelated org queues, exact frontiers, and old allocation floor.
pub(crate) async fn reset_personal_candidate_generation(
    old_stopped_home: &Path,
    candidate_home: &Path,
    plan: &AdoptionPlan,
    retained: &RetainedHome,
    replacement_frontier: &BTreeMap<String, u64>,
) -> Result<String, String> {
    let keys = identity_keys(old_stopped_home)?;
    let (prior_raw, prior) = open_peer_store(old_stopped_home, &keys).await?;
    let (raw, candidate) = open_peer_store(candidate_home, &keys).await?;
    let (inbound_digest, inbound_targets) =
        fold_db::sync::cloud_purge_peer::inbound_share::preserve_inbound_shares(&prior, &candidate)
            .await
            .map_err(message)?;
    let mut orgs = fold_db::sharing::org_sync_target::list_org_sync_targets_in_ops(&prior)
        .await
        .map_err(message)?;
    // This maintenance router must retain inactive target queues too. No
    // worker can publish with this helper, and real registry flags stay intact.
    for org in &mut orgs {
        org.active = true;
    }
    let shares = fold_db::sharing::store::list_share_rules_in_ops(&prior)
        .await
        .map_err(message)?;
    let engine = stopped_engine(
        old_stopped_home,
        plan,
        &keys,
        &candidate,
        raw.clone(),
        &orgs,
        &shares,
    )
    .await?;
    let org_ids = orgs
        .iter()
        .map(|target| target.org_hash.clone())
        .collect::<Vec<_>>();
    let digest = engine
        .preserve_cloud_purge_org_queues(
            (prior.namespaced_store(), prior_raw),
            plan,
            retained,
            &org_ids,
            &inbound_targets,
            replacement_frontier,
        )
        .await
        .map_err(message)?;
    // Subscription consent belongs to this device; inherited sender/device
    // consent from the selected snapshot cannot take its place.
    let subscription_digest = copy_peer_subscriptions(&prior, &candidate).await?;
    candidate
        .namespaced_store()
        .restore_durability_barrier()
        .await
        .map_err(message)?;
    raw.verify_integrity().map_err(message)?;
    Ok(fold_db::hex::sha256_hex(serde_json::to_vec(&serde_json::json!({
        "sync_queues":digest, "inbound_data":inbound_digest, "inbound_subscriptions":subscription_digest,
    })).map_err(message)?))
}

async fn stopped_engine(
    old_stopped_home: &Path,
    plan: &AdoptionPlan,
    keys: &E2eKeys,
    candidate: &DbOperations,
    raw: Arc<LastStoreNamespacedStore>,
    orgs: &[OrgSyncTarget],
    shares: &[fold_db::sharing::types::ShareRule],
) -> Result<SyncEngine, String> {
    let peer = CloudPurgePeer::open(
        CloudPurgePeer::directory_for_home(old_stopped_home).map_err(message)?,
        plan.source_id.clone(),
    )
    .map_err(message)?;
    peer.bind_local_account(&plan.account).map_err(message)?;
    let http = Arc::new(reqwest::Client::new());
    let auth = AuthClient::new(
        Arc::clone(&http),
        String::new(),
        SyncAuth::ApiKey(String::new()),
    )
    .without_db_auto_claim()
    .with_cloud_purge_peer(peer);
    // This constructor does not start workers. The signer cannot publish: the
    // helper performs only archive and storage operations, with no endpoint.
    let signer = Arc::new(fold_db::security::Ed25519KeyPair::generate().map_err(message)?);
    let mut engine = SyncEngine::new(
        plan.source_id.clone(),
        Arc::new(LocalCryptoProvider::from_key(keys.encryption_key())),
        fold_db::sync::s3::S3Client::new(http),
        auth,
        candidate.namespaced_store(),
        SyncConfig::default(),
        signer,
    );
    engine.set_at_rest_key(keys.encryption_key());
    engine.set_cursor_store(raw.clone());
    engine.arm_stopped_cloud_purge_interlock().await;
    let partitioner = SyncPartitioner::new_with_orgs(shares, orgs);
    let mut targets = Vec::new();
    for target in orgs {
        targets.push(SyncTarget {
            label: format!("org:{}", target.slug),
            prefix: target.org_hash.clone(),
            crypto: Arc::new(LocalCryptoProvider::from_key(org_key(target)?)),
            read_binding: None,
        });
    }
    for rule in shares.iter().filter(|rule| rule.active) {
        let key: [u8; 32] = rule
            .share_e2e_secret
            .as_slice()
            .try_into()
            .map_err(|_| "the prior shared target key is invalid")?;
        targets.push(SyncTarget {
            label: format!("share:{}", rule.rule_id),
            prefix: rule.share_prefix.clone(),
            crypto: Arc::new(LocalCryptoProvider::from_key(key)),
            read_binding: None,
        });
    }
    engine.configure_targets(partitioner, targets).await;
    Ok(engine)
}

async fn copy_peer_subscriptions(
    prior: &DbOperations,
    candidate: &DbOperations,
) -> Result<String, String> {
    use sha2::Digest;
    let mut digest = sha2::Sha256::new();
    let source = prior
        .open_namespace("share_subscriptions")
        .await
        .map_err(message)?;
    let destination = candidate
        .open_namespace("share_subscriptions")
        .await
        .map_err(message)?;
    loop {
        let rows = destination
            .scan_range_paged(&[], &[0xff], 128)
            .await
            .map_err(message)?;
        if rows.is_empty() {
            break;
        }
        destination
            .batch_delete(rows.into_iter().map(|(key, _)| key).collect())
            .await
            .map_err(message)?;
    }
    let mut cursor = Vec::new();
    loop {
        let rows = source
            .scan_range_paged(&cursor, &[0xff], 128)
            .await
            .map_err(message)?;
        if rows.is_empty() {
            break;
        }
        cursor = rows.last().expect("nonempty page").0.clone();
        cursor.push(0);
        for (key, value) in &rows {
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        }
        destination.batch_put(rows).await.map_err(message)?;
    }
    destination.flush().await.map_err(message)?;
    Ok(fold_db::hex::hex_lower(digest.finalize()))
}
