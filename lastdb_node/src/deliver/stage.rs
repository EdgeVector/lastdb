use super::*;

use super::legs::*;
use super::types::*;
use fold_db::sharing::delivery_wire::LastDbSlicePayload;

/// `POST /api/sharing/deliver` — stage a snapshot delivery (no network).
pub async fn execute_stage_delivery(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let body: StageBody = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => return error_json(400, &format!("invalid body: {e}"), ctx),
    };
    if let Err(e) = validate_stage_recipient(&body) {
        return error_json(400, &e, ctx);
    }

    let legs = match resolve_legs(host, &body) {
        Ok(l) => l,
        Err(e) => return render(Err(e), ctx),
    };

    let payload = match materialize_nonempty(host, &legs, ctx).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let (records, preview) = build_preview(&payload);

    let SealedSlice {
        content_key,
        envelope,
        artifact,
    } = match sign_and_encrypt_slice(payload, host, ctx) {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    let delivery_id = Uuid::new_v4().to_string();
    let staged = StagedDeliveryArtifact {
        envelope,
        content_key: content_key.to_vec(),
    };
    if let Err(e) =
        store_staged_delivery_artifact_in_ops(host.db.db_ops(), &delivery_id, &staged).await
    {
        return error_json(500, &format!("store staged artifact: {e}"), ctx);
    }

    // Spec retained for preview/provenance; Mini stages from legs only.
    let primary = legs.first().expect("legs non-empty");
    let delivery = PendingDelivery {
        delivery_id: delivery_id.clone(),
        recipient_pubkey: body.recipient_pubkey,
        recipient_display_name: body
            .recipient_display_name
            .unwrap_or_else(|| "recipient".into()),
        spec: DeliverySpec::Query {
            query: primary.to_query(),
        },
        mode: DeliveryMode::Snapshot,
        scope: None,
        records,
        preview,
        artifact,
        status: "pending".to_string(),
        created_at: unix_secs(),
        decided_at: None,
        messaging_public_key: Some(body.messaging_public_key),
        messaging_pseudonym: Some(body.messaging_pseudonym),
    };
    if let Err(e) = store_pending_delivery_in_ops(host.db.db_ops(), &delivery).await {
        let _ = remove_pending_delivery_in_ops(host.db.db_ops(), &delivery_id).await;
        return error_json(500, &format!("store pending delivery: {e}"), ctx);
    }

    ok_json(
        &serde_json::json!({
            "delivery": delivery,
            "note": "staged only — no network until POST /api/sharing/deliveries/{id}/approve",
        }),
        ctx,
    )
}

/// `GET /api/sharing/deliveries` — list pending staged deliveries.
pub async fn execute_list_deliveries(ctx: &AccessContext, host: &Host) -> UdsResponse {
    match list_pending_deliveries_in_ops(host.db.db_ops()).await {
        Ok(deliveries) => ok_json(&serde_json::json!({ "deliveries": deliveries }), ctx),
        Err(e) => error_json(500, &format!("list deliveries: {e}"), ctx),
    }
}

/// Build preview samples from atoms (first 3 record keys).
fn build_preview(payload: &LastDbSlicePayload) -> (Vec<DeliveryRecord>, DeliveryPreview) {
    let mut field_names: HashSet<String> = HashSet::new();
    let mut records: Vec<DeliveryRecord> = Vec::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut by_record: BTreeMap<(String, String), BTreeMap<String, serde_json::Value>> =
        BTreeMap::new();
    let atoms: BTreeMap<_, _> = payload
        .atoms
        .iter()
        .map(|a| (a.atom_ref.clone(), a.value.clone()))
        .collect();
    for mol in &payload.molecules {
        field_names.insert(mol.field_name.clone());
        let key = (mol.schema_name.clone(), mol.record_key.clone());
        if seen.insert(key.clone()) {
            records.push(DeliveryRecord {
                schema_name: mol.schema_name.clone(),
                record_key: mol.record_key.clone(),
                fields: None,
            });
        }
        if let Some(v) = atoms.get(&mol.atom_ref) {
            by_record
                .entry(key)
                .or_default()
                .insert(mol.field_name.clone(), v.clone());
        }
    }
    let mut sample = Vec::new();
    for ((schema, record_key), fields) in by_record.into_iter().take(3) {
        sample.push(DeliverySampleRecord {
            schema_name: schema,
            record_key,
            fields,
        });
    }
    let mut fields: Vec<String> = field_names.into_iter().collect();
    fields.sort();
    let preview = DeliveryPreview {
        query_label: payload.provenance.source.clone(),
        fields,
        record_count: records.len(),
        sample,
    };
    (records, preview)
}
