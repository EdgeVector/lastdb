//! Foreign share notices authorize only frozen reads of that exact share.

use crate::sync::cloud_purge::protocol::{CloudAccountKind, CloudControlGet};
use crate::sync::cloud_share::{held, sender_hash, ShareReadNotice};
use crate::sync::SyncResult;

pub(super) fn notice(response: &CloudControlGet, prefix: &str) -> SyncResult<ShareReadNotice> {
    if response.notification_scope.as_deref() != Some("share_read")
        || response.scope_root.as_deref() != Some(prefix)
    {
        return Err(held("the share notice does not bind this exact read scope"));
    }
    let (account, generation, operation_id) =
        super::recipient_delivery::resolved_notification(response)?;
    let data_generation = response
        .notification
        .as_ref()
        .and_then(|notification| notification.data_generation)
        .ok_or_else(|| held("the share notice physical generation is absent"))?;
    if account.kind != CloudAccountKind::Personal
        || sender_hash(prefix) != Some(account.id.as_str())
        || data_generation > generation
        || (generation == 0 && data_generation != 0)
    {
        return Err(held(
            "the share notice account or physical generation differs",
        ));
    }
    Ok(ShareReadNotice {
        account,
        generation,
        data_generation,
        operation_id,
    })
}

pub(super) fn bind_read(
    body: &mut serde_json::Value,
    generation: u64,
    data_generation: u64,
) -> SyncResult<()> {
    for (field, expected) in [
        ("cloud_generation", generation),
        ("cloud_read_data_generation", data_generation),
    ] {
        if body
            .get(field)
            .is_some_and(|value| value.as_u64() != Some(expected))
        {
            return Err(held(
                "the share read generation changed; refresh its receiver snapshot",
            ));
        }
        body[field] = expected.into();
    }
    Ok(())
}
