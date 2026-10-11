//! Approval retries use the exact same durable ciphertext and delivery nonce.

use super::*;

static DECISIONS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) async fn decision_guard() -> tokio::sync::MutexGuard<'static, ()> {
    DECISIONS.lock().await
}

/// The caller serializes approve/reject before it reads the pending record.
pub(super) async fn prepare(
    host: &Host,
    delivery: &mut PendingDelivery,
    target_pk: &[u8; 32],
) -> Result<String, String> {
    if let Some(blob) = &delivery.cloud_delivery_ciphertext {
        check_size(blob)?;
        return Ok(blob.clone());
    }
    let staged = get_staged_delivery_artifact_in_ops(host.db.db_ops(), &delivery.delivery_id)
        .await
        .map_err(|error| format!("get staged artifact: {error}"))?
        .ok_or("pending delivery lacks its staged artifact; re-stage")?;
    let payload = DeliverySlicePayload {
        message_type: "delivery_slice".into(),
        content_key: B64.encode(&staged.content_key),
        envelope: staged.envelope,
        artifact: delivery.artifact.clone(),
    };
    let sealed = seal_and_encrypt_message(target_pk, &payload, host.keypair.as_ref())
        .map_err(|error| format!("seal delivery failed: {error}"))?;
    let blob = B64.encode(sealed);
    check_size(&blob)?;
    delivery.cloud_delivery_ciphertext = Some(blob.clone());
    store_pending_delivery_in_ops(host.db.db_ops(), delivery)
        .await
        .map_err(|error| format!("save delivery ciphertext: {error}"))?;
    host.db
        .db_ops()
        .flush()
        .await
        .map_err(|error| format!("flush delivery ciphertext: {error}"))?;
    Ok(blob)
}

fn check_size(blob: &str) -> Result<(), String> {
    if blob.is_empty() || blob.len() > MAX_BLOB_B64_CHARS {
        return Err(format!("sealed delivery exceeds the {MAX_BLOB_B64_CHARS} character cap; shrink the query slice"));
    }
    Ok(())
}
