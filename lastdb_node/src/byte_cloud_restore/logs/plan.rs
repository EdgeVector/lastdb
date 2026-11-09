use super::*;
use cloud_backup_protocol::v2::AcceptedBackup;
use cloud_backup_protocol::Digest;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    pub version: u16,
    pub head: Digest,
    pub base: AcceptedBackup,
    pub links: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached: Option<CachedStart>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CachedStart {
    pub accepted: AcceptedBackup,
    pub position: LogPosition,
}

pub(super) async fn prepare(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    cache: Option<&GuestReplayCache<'_>>,
) -> Result<Plan> {
    let path = session.root.join("chain/plan.json");
    if path.try_exists()? {
        let plan: Plan = local::read_json(&path, 256 * 1024)?;
        if plan.version != 1
            || plan.head != session.accepted().commit_sha256
            || plan.base != latest.base
            || plan.links == 0
            || plan.links > MAX_CHAIN
            || plan.cached.as_ref().is_some_and(|start| {
                cache.is_none_or(|cache| {
                    start.accepted.head() != *cache.head
                        || !cache
                            .through
                            .map_or(start.accepted == latest.base, |saved| {
                                *saved == start.position
                            })
                })
            })
        {
            return Err(RestoreError::Invalid("saved mutation restore plan"));
        }
        return Ok(plan);
    }
    let mut accepted = session.accepted().clone();
    let mut root = latest.clone();
    let mut count = 0;
    let cached = loop {
        if count >= MAX_CHAIN {
            return Err(RestoreError::Limit("mutation restore chain"));
        }
        root.validate_for(&accepted.begin)
            .map_err(|_| RestoreError::Invalid("mutation checkpoint context"))?;
        if root.base != latest.base
            || root.anchor != latest.anchor
            || root.recovery != latest.recovery
            || root.previous.ordinal.get() >= accepted.ordinal.get()
        {
            return Err(RestoreError::Invalid("mutation predecessor chain"));
        }
        let link = link_path(session, count);
        if link.try_exists()? {
            let prior: AcceptedBackup = local::read_json(&link, 256 * 1024)?;
            if prior != accepted {
                return Err(RestoreError::Invalid("saved mutation ancestor"));
            }
        } else {
            save(session, &link, &accepted)?;
        }
        count += 1;
        super::cache::clear(session)?;
        if let Some(cached) = cached_predecessor(cache, &root)? {
            break Some(cached);
        }
        if root.previous == latest.base {
            break None;
        }
        let ancestor = session.ancestor(root.previous.clone())?;
        let (_, bytes) = super::super::closure::load(&ancestor, client).await?;
        let previous =
            MutationLogRoot::decode(&bytes, decryption.content_key(), &ancestor.accepted().begin)
                .map_err(|_| RestoreError::Invalid("mutation ancestor root"))?;
        root.validate_previous(&previous)
            .map_err(|_| RestoreError::Invalid("mutation ancestor prefix"))?;
        accepted = root.previous;
        root = previous;
    };
    let plan = Plan {
        version: 1,
        head: session.accepted().commit_sha256.clone(),
        base: latest.base.clone(),
        links: count,
        cached,
    };
    save(session, &path, &plan)?;
    Ok(plan)
}

fn cached_predecessor(
    cache: Option<&GuestReplayCache<'_>>,
    root: &MutationLogRoot,
) -> Result<Option<CachedStart>> {
    let Some(cache) = cache.filter(|cache| root.previous.head() == *cache.head) else {
        return Ok(None);
    };
    if !cache
        .through
        .map_or(root.previous == root.base, |saved| *saved == root.from)
    {
        return Err(RestoreError::Invalid(
            "cached mutation checkpoint predecessor",
        ));
    }
    Ok(Some(CachedStart {
        accepted: root.previous.clone(),
        position: root.from.clone(),
    }))
}
pub(super) fn link_path(session: &RestoreSession, index: u32) -> std::path::PathBuf {
    session.root.join(format!("chain/link-{index:08}.json"))
}
pub(super) fn save(
    session: &RestoreSession,
    path: &std::path::Path,
    value: &impl serde::Serialize,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    use std::sync::atomic::Ordering;
    let previous = match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => meta
            .blocks()
            .checked_mul(512)
            .ok_or(RestoreError::Limit("chain metadata size"))?,
        Ok(_) => return Err(RestoreError::Invalid("chain metadata file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    session.remove_spool_file(&path.with_extension("pending"))?;
    session.reserve_spool(2 * 256 * 1024)?;
    if let Err(error) = local::atomic_json(path, value) {
        if path.with_extension("pending").try_exists()? {
            session.add_spool_file(&path.with_extension("pending"))?;
        }
        return Err(error);
    }
    session.spool_bytes.fetch_sub(previous, Ordering::Relaxed);
    session.add_spool_file(path)
}
