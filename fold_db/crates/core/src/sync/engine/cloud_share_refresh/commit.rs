//! One durable binding exposes a complete union after every seed flush.

use super::*;

pub(super) async fn publish(
    engine: &SyncEngine,
    shares: &[(SyncTarget, Option<ShareReadNotice>)],
    binding: &ReceiverBinding,
    previous: Option<&ReceiverBinding>,
    cursors: &BTreeMap<String, u64>,
) -> SyncResult<()> {
    binding.validate()?;
    let notices = futures::future::join_all(
        shares
            .iter()
            .map(|(target, _)| engine.auth.cloud_target_preflight(target)),
    )
    .await;
    let fresh = shares
        .iter()
        .zip(notices)
        .map(|((target, _), notice)| Ok((target.clone(), notice?)))
        .collect::<SyncResult<Vec<_>>>()?;
    let notice = stage::consistent_notice(&fresh)?;
    if notice.account != binding.account
        || notice.generation != binding.generation
        || notice.data_generation != binding.data_generation
        || notice.operation_id != binding.operation_id
    {
        return Err(held(
            "the sender generation changed before the receiver switch",
        ));
    }
    let _config = engine.target_config_lock.lock().await;
    let mut targets = engine.targets.lock().await;
    validate_targets(&targets, shares)?;
    let cursors = save_cursors(engine, shares, binding, cursors).await?;
    save_binding(engine, binding, previous).await?;
    for target in targets.iter_mut().filter(|target| {
        target.read_binding.is_some()
            && sender_hash(&target.prefix) == Some(binding.sender_hash.as_str())
    }) {
        let grant = target
            .read_binding
            .as_ref()
            .and_then(|old| old.share_grant.clone());
        target.read_binding = Some(binding.target(grant));
    }
    engine
        .download_cursors
        .lock()
        .await
        .extend(cursors.iter().map(|(prefix, seq)| (prefix.clone(), *seq)));
    engine
        .target_config_generation
        .fetch_add(1, std::sync::atomic::Ordering::Release);
    Ok(())
}

async fn save_cursors(
    engine: &SyncEngine,
    shares: &[(SyncTarget, Option<ShareReadNotice>)],
    binding: &ReceiverBinding,
    cursors: &BTreeMap<String, u64>,
) -> SyncResult<BTreeMap<String, u64>> {
    let cursors = shares
        .iter()
        .filter_map(|(target, _)| {
            cursors.get(&target.prefix).map(|seq| {
                let mut target = target.clone();
                let grant = target
                    .read_binding
                    .as_ref()
                    .and_then(|old| old.share_grant.clone());
                target.read_binding = Some(binding.target(grant));
                (target.cursor_key(), *seq)
            })
        })
        .collect::<BTreeMap<_, _>>();
    let cursor_store = engine.cursor_store.open_namespace("sync_cursors").await?;
    cursor_store
        .batch_put(
            cursors
                .iter()
                .map(|(prefix, seq)| {
                    Ok((
                        format!("cursor:{prefix}").into_bytes(),
                        engine.encode_cursor(*seq).map_err(SyncError::Storage)?,
                    ))
                })
                .collect::<SyncResult<Vec<_>>>()?,
        )
        .await?;
    engine.cursor_store.restore_durability_barrier().await?;
    Ok(cursors)
}

async fn save_binding(
    engine: &SyncEngine,
    binding: &ReceiverBinding,
    previous: Option<&ReceiverBinding>,
) -> SyncResult<()> {
    let history = engine
        .store
        .open_namespace("sync_foreign_share_history")
        .await?;
    if let Some(previous) = previous {
        let bytes = serde_json::to_vec(previous)?;
        history
            .put(
                format!("{}:{}", binding.sender_hash, crate::hex::sha256_hex(&bytes)).as_bytes(),
                bytes,
            )
            .await?;
    }
    let receivers = engine
        .store
        .open_namespace(crate::sync::cloud_share::RECEIVER_BINDINGS_NAMESPACE)
        .await?;
    receivers
        .put(binding.sender_hash.as_bytes(), serde_json::to_vec(binding)?)
        .await?;
    engine.store.restore_durability_barrier().await?;
    Ok(())
}

fn validate_targets(
    current: &[SyncTarget],
    shares: &[(SyncTarget, Option<ShareReadNotice>)],
) -> SyncResult<()> {
    let sender = sender_hash(
        &shares
            .first()
            .ok_or_else(|| held("the sender share union is empty"))?
            .0
            .prefix,
    )
    .ok_or_else(|| held("the sender share scope is invalid"))?;
    let current = current
        .iter()
        .filter(|target| {
            target.read_binding.is_some() && sender_hash(&target.prefix) == Some(sender)
        })
        .map(|target| {
            (
                &target.prefix,
                target
                    .read_binding
                    .as_ref()
                    .and_then(|binding| binding.share_grant.as_ref()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let expected = shares
        .iter()
        .map(|(target, _)| {
            (
                &target.prefix,
                target
                    .read_binding
                    .as_ref()
                    .and_then(|binding| binding.share_grant.as_ref()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if serde_json::to_vec(&current)? != serde_json::to_vec(&expected)? {
        return Err(held(
            "the active sender subscriptions changed before the receiver switch",
        ));
    }
    Ok(())
}
