use super::*;

/// `POST /api/sharing/deliveries/{id}/approve` — seal + send via Exemem messaging.
pub async fn execute_approve_delivery(
    delivery_id: &str,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let delivery = match get_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
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

    let staged = match get_staged_delivery_artifact_in_ops(host.db.db_ops(), delivery_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return error_json(
                500,
                "pending snapshot delivery missing staged wire artifact; re-stage",
                ctx,
            )
        }
        Err(e) => return error_json(500, &format!("get staged artifact: {e}"), ctx),
    };

    let slice_payload = DeliverySlicePayload {
        message_type: "delivery_slice".to_string(),
        content_key: B64.encode(&staged.content_key),
        envelope: staged.envelope,
        artifact: delivery.artifact.clone(),
    };

    let sealed = match seal_and_encrypt_message(&target_pk, &slice_payload, host.keypair.as_ref()) {
        Ok(b) => b,
        Err(e) => return error_json(500, &format!("seal delivery failed: {e}"), ctx),
    };
    let encrypted_blob = B64.encode(&sealed);
    if encrypted_blob.len() > MAX_BLOB_B64_CHARS {
        return error_json(
            400,
            &format!(
                "sealed delivery is {} base64 chars (max {MAX_BLOB_B64_CHARS}); shrink the query slice",
                encrypted_blob.len()
            ),
            ctx,
        );
    }

    let cloud = match require_cloud_sync(host, ctx) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    if let Err(e) = post_messaging_connect(
        &cloud.api_url,
        &cloud.api_key,
        &messaging_pseudonym,
        &encrypted_blob,
    )
    .await
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

fn require_cloud_sync(host: &Host, ctx: &AccessContext) -> Result<CloudSyncConfig, UdsResponse> {
    match load_cloud_sync(&host.home) {
        Some(c) if !c.api_url.is_empty() && !c.api_key.is_empty() => Ok(c),
        Some(_) => Err(error_json(
            400,
            "cloud_sync.json present but api_key empty — re-run lastdb connect / re-register",
            ctx,
        )),
        None => Err(error_json(
            400,
            "cloud_sync.json required to send via Exemem messaging",
            ctx,
        )),
    }
}

async fn post_messaging_connect(
    api_url: &str,
    api_key: &str,
    target_pseudonym: &str,
    encrypted_blob: &str,
) -> Result<(), String> {
    let base = api_url.trim_end_matches('/');
    // Exemem HTTP API mounts messaging under `/api/...` (see exemem-stack
    // MessagingConnect route). Do not call bare `/messaging/connect` — that
    // 404s on API Gateway.
    let url = if base.ends_with("/api") {
        format!("{base}/messaging/connect")
    } else {
        format!("{base}/api/messaging/connect")
    };
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .header("X-API-Key", api_key)
        .json(&serde_json::json!({
            "target_pseudonym": target_pseudonym,
            "encrypted_blob": encrypted_blob,
        }))
        .send()
        .await
        .map_err(|e| format!("http error: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("status {status}: {body}"));
    }
    Ok(())
}
