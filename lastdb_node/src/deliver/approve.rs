use super::*;

/// `POST /api/sharing/deliveries/{id}/approve` — seal + send via Exemem messaging.
pub async fn execute_approve_delivery(
    delivery_id: &str,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let _decision = super::ciphertext::decision_guard().await;
    let mut delivery = match get_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
        Ok(Some(d)) => d,
        Ok(None) => return error_json(404, &format!("Delivery not found: {delivery_id}"), ctx),
        Err(e) => return error_json(500, &format!("get delivery: {e}"), ctx),
    };
    if delivery.status != "pending" {
        return error_json(
            400,
            &format!("delivery {delivery_id} is already {}", delivery.status),
            ctx,
        );
    }
    let (target_pk, messaging_pseudonym) = match delivery_target(&delivery, ctx) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let encrypted_blob = match super::ciphertext::prepare(host, &mut delivery, &target_pk).await {
        Ok(blob) => blob,
        Err(error) => return error_json(500, &error, ctx),
    };

    if let Err(e) =
        super::cloud_generation::send(host, &delivery, &messaging_pseudonym, &encrypted_blob).await
    {
        return error_json(502, &format!("messaging send failed: {e}"), ctx);
    }

    if let Err(e) = remove_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
        return error_json(500, &format!("sent but failed to clear outbox: {e}"), ctx);
    }

    ok_json(
        &serde_json::json!({
            "delivery_id": delivery_id,
            "shared": delivery.records.len(),
            "message_type": "delivery_slice",
        }),
        ctx,
    )
}

/// `POST /api/sharing/deliveries/{id}/reject` — discard staged delivery.
pub async fn execute_reject_delivery(
    delivery_id: &str,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let _decision = super::ciphertext::decision_guard().await;
    match get_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
        Ok(None) => return error_json(404, &format!("Delivery not found: {delivery_id}"), ctx),
        Ok(Some(_)) => {}
        Err(e) => return error_json(500, &format!("get delivery: {e}"), ctx),
    }
    if let Err(e) = remove_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
        return error_json(500, &format!("remove delivery: {e}"), ctx);
    }
    ok_json(
        &serde_json::json!({ "delivery_id": delivery_id, "status": "rejected" }),
        ctx,
    )
}

/// Messaging key + pseudonym recorded at stage time; both are required to send.
fn delivery_target(
    delivery: &PendingDelivery,
    ctx: &AccessContext,
) -> Result<([u8; 32], String), UdsResponse> {
    let Some(messaging_pk_b64) = delivery.messaging_public_key.as_ref() else {
        return Err(error_json(
            400,
            "delivery has no messaging_public_key — re-stage with messaging keys",
            ctx,
        ));
    };
    let Some(messaging_pseudonym) = delivery.messaging_pseudonym.as_ref() else {
        return Err(error_json(
            400,
            "delivery has no messaging_pseudonym — re-stage with messaging keys",
            ctx,
        ));
    };
    let pk_bytes = match B64.decode(messaging_pk_b64.as_bytes()) {
        Ok(b) if b.len() == 32 => b,
        _ => {
            return Err(error_json(
                500,
                "stored messaging_public_key is invalid",
                ctx,
            ))
        }
    };
    let mut target_pk = [0u8; 32];
    target_pk.copy_from_slice(&pk_bytes);
    Ok((target_pk, messaging_pseudonym.clone()))
}
