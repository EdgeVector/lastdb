use super::*;

use super::legs::*;
use super::types::*;

/// `POST /api/sharing/snapshot` — seal a query snapshot for object-store overwrite.
///
/// Does **not** append to the Exemem messaging mailbox. Callers PUT
/// `encrypted_blob` to `s3://…/delivery-snapshots/{snapshot_key}` (or equivalent)
/// replacing any previous object at that key.
pub async fn execute_publish_snapshot(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let body: StageBody = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => return error_json(400, &format!("invalid body: {e}"), ctx),
    };
    let messaging_pk = match validate_stage_recipient(&body) {
        Ok(pk) => pk,
        Err(e) => return error_json(400, &e, ctx),
    };
    let legs = match resolve_legs(host, &body) {
        Ok(l) => l,
        Err(e) => return render(Err(e), ctx),
    };
    if legs.is_empty() {
        return error_json(400, "snapshot requires at least one query leg", ctx);
    }

    let canonical = canonical_query_from_legs(&legs);
    let snapshot_key = match snapshot_key_for(&canonical, &body.messaging_pseudonym) {
        Ok(k) => k,
        Err(e) => return error_json(500, &e, ctx),
    };

    let payload = match materialize_nonempty(host, &legs, ctx).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let record_count = {
        let mut seen: HashSet<(String, String)> = HashSet::new();
        for mol in &payload.molecules {
            seen.insert((mol.schema_name.clone(), mol.record_key.clone()));
        }
        seen.len()
    };

    let SealedSlice {
        content_key,
        envelope,
        artifact,
    } = match sign_and_encrypt_slice(payload, host, ctx) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let slice_payload = DeliverySlicePayload {
        message_type: "delivery_slice".to_string(),
        content_key: B64.encode(content_key),
        envelope,
        artifact,
    };
    let sealed =
        match seal_and_encrypt_message(&messaging_pk, &slice_payload, host.keypair.as_ref()) {
            Ok(b) => b,
            Err(e) => return error_json(500, &format!("seal snapshot failed: {e}"), ctx),
        };
    let encrypted_blob = B64.encode(&sealed);
    if encrypted_blob.len() > MAX_SNAPSHOT_BLOB_B64_CHARS {
        return error_json(
            400,
            &format!(
                "sealed snapshot is {} base64 chars (max {MAX_SNAPSHOT_BLOB_B64_CHARS}); shrink the query",
                encrypted_blob.len()
            ),
            ctx,
        );
    }

    ok_json(
        &serde_json::json!({
            "snapshot_key": snapshot_key,
            "recipient_id": body.messaging_pseudonym,
            "canonical_query": canonical,
            "encrypted_blob": encrypted_blob,
            "record_count": record_count,
            "message_type": "delivery_slice",
            "transport": "object_snapshot",
            "note": "overwrite object-store key delivery-snapshots/{snapshot_key}; no messaging append",
        }),
        ctx,
    )
}
