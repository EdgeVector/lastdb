//! Recipient notices carry no owner data and grant no local adoption.

use super::held;
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudControlGet};
use crate::sync::error::{SyncError, SyncResult};

pub(in crate::sync::auth) fn resolved_recipient_notification(
    response: &CloudControlGet,
) -> SyncResult<(CloudAccount, u64, Option<String>)> {
    if response
        .notification_scope
        .as_deref()
        .is_some_and(|scope| scope != "recipient_delivery")
        || response.scope_root.is_some()
    {
        return Err(held(
            "a share-read notice cannot authorize a recipient delivery",
        ));
    }
    resolved_notification(response)
}

pub(in crate::sync::auth) fn resolved_ready(
    response: &CloudControlGet,
) -> SyncResult<(CloudAccount, u64, Option<String>)> {
    if response.notification_only || response.notification.is_some() {
        return Err(held(
            "sender admission requires the authorized full account control",
        ));
    }
    let account = response
        .account
        .as_ref()
        .or_else(|| response.control.as_ref().map(|control| &control.account))
        .ok_or_else(|| held("recipient control did not resolve its account"))?;
    if account.id.is_empty() {
        return Err(held("recipient account identity is empty"));
    }
    if !response.protocol_required {
        if response.generation != 0 || response.control.is_some() {
            return Err(held("recipient legacy control response is invalid"));
        }
        return Ok((account.clone(), 0, None));
    }
    let control = response
        .control
        .as_ref()
        .ok_or_else(|| held("recipient permanent control is absent"))?;
    control
        .validate_identity()
        .map_err(SyncError::CloudPurgeHeld)?;
    if control.account != *account
        || control.generation != response.generation
        || !control.permits_local_adoption()
    {
        return Err(held("the recipient or sender account remains held"));
    }
    Ok((
        account.clone(),
        control.generation,
        Some(control.operation_id.clone()),
    ))
}

pub(in crate::sync::auth) fn resolved_notification(
    response: &CloudControlGet,
) -> SyncResult<(CloudAccount, u64, Option<String>)> {
    if !response.notification_only || response.control.is_some() {
        return Err(held(
            "recipient admission requires a notification without account data",
        ));
    }
    let notification = response
        .notification
        .as_ref()
        .ok_or_else(|| held("recipient generation notification is absent"))?;
    if response.account.as_ref() != Some(&notification.account)
        || notification.account.id.is_empty()
        || response.generation != notification.generation
        || !notification.open_for_writes
    {
        return Err(held("recipient generation notification is held or differs"));
    }
    if response.protocol_required {
        let operation = notification
            .operation_id
            .as_deref()
            .ok_or_else(|| held("recipient operation identity is absent"))?;
        if notification.generation == 0
            || uuid::Uuid::parse_str(operation).is_err()
            || !matches!(notification.phase.as_str(), "ready" | "cancelled")
        {
            return Err(held("recipient permanent notification is invalid"));
        }
    } else if notification.generation != 0
        || notification.operation_id.is_some()
        || notification.phase != "legacy"
    {
        return Err(held("recipient legacy notification is invalid"));
    }
    Ok((
        notification.account.clone(),
        notification.generation,
        notification.operation_id.clone(),
    ))
}
