//! New deliveries freeze the recipient generation before durable staging.

use super::*;
use fold_db::sync::auth::{AuthClient, RecipientDeliveryBinding, RecipientDeliveryTarget};

pub(super) async fn auth(host: &Host) -> Result<AuthClient, String> {
    let engine = crate::cloud_purge_runtime::peer_engine(host).await?;
    Ok(engine.cloud_purge_auth())
}

pub(super) async fn prepare(
    host: &Host,
    target_pseudonym: &str,
    delivery_id: &str,
) -> Result<Value, String> {
    let auth = auth(host).await?;
    let binding = auth
        .prepare_recipient_delivery(
            RecipientDeliveryTarget::Messaging {
                target_pseudonym: target_pseudonym.into(),
            },
            delivery_id,
        )
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_value(binding).map_err(|_| "recipient delivery binding cannot be saved".into())
}

pub(super) async fn send(
    host: &Host,
    delivery: &PendingDelivery,
    target_pseudonym: &str,
    encrypted_blob: &str,
) -> Result<(), String> {
    let binding = delivery
        .cloud_delivery_binding
        .as_ref()
        .map(|value| {
            serde_json::from_value::<RecipientDeliveryBinding>(value.clone())
                .map_err(|_| "the staged recipient generation binding is invalid".to_string())
        })
        .transpose()?;
    let auth = auth(host).await?;
    auth.send_recipient_messaging(
        target_pseudonym,
        &delivery.delivery_id,
        binding.as_ref(),
        encrypted_blob,
    )
    .await
    .map_err(|error| error.to_string())
}
