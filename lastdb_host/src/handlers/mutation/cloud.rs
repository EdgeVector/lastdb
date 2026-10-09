//! Cloud-capture policy and cloud receipt rendering for mutations.

use super::*;

pub(in super::super) fn is_durable_delete(
    mutation_type: MutationType,
    durability: MutationDurability,
) -> bool {
    matches!(mutation_type, MutationType::Delete)
        && matches!(durability, MutationDurability::Durable)
}

pub(in super::super) fn cloud_capture_policy(
    mutation_type: MutationType,
    durability: MutationDurability,
    cloud_publication: Option<MutationCloudPublication>,
    must_exist: Option<bool>,
) -> Result<CloudCapturePolicy, HostError> {
    if cloud_publication.is_some()
        && (!matches!(mutation_type, MutationType::Delete)
            || !matches!(durability, MutationDurability::Durable))
    {
        return Err(HostError::new(
            400,
            "cloud_publication is only valid on a durable delete",
        ));
    }
    if cloud_publication.is_some() && must_exist == Some(true) {
        return Err(HostError::new(
            400,
            "cloud_publication wait cannot be combined with must_exist: true; exact delete retries must be idempotent",
        ));
    }
    if is_durable_delete(mutation_type, durability) {
        return Ok(match cloud_publication {
            Some(MutationCloudPublication::Wait) => CloudCapturePolicy::WaitForPublication {
                timeout: MUTATION_CLOUD_PUBLICATION_TIMEOUT,
            },
            None => CloudCapturePolicy::Durable,
        });
    }
    Ok(CloudCapturePolicy::Async)
}

pub(in super::super) fn cloud_receipt_json(
    receipt: &CloudMutationReceipt,
    mutation_id: &str,
) -> (Value, Value) {
    let binding_error = (receipt.mutation_uuid != mutation_id).then(|| {
        format!(
            "cloud receipt mutation UUID '{}' does not match response mutation_id '{mutation_id}'",
            receipt.mutation_uuid
        )
    });
    let exact_targets = !receipt.targets.is_empty()
        && receipt.targets.iter().all(|target| {
            !target.target_id.is_empty() && !target.writer_id.is_empty() && target.frontier > 0
        });
    let claims_exact_publication = matches!(
        receipt.publication_state,
        CloudPublicationState::Pending | CloudPublicationState::Published
    );
    let missing_coordinate = claims_exact_publication && !exact_targets;
    let capture_state = if binding_error.is_some() {
        CloudCaptureState::Failed
    } else {
        receipt.capture_state
    };
    let publication_state = if binding_error.is_some() || missing_coordinate {
        CloudPublicationState::Failed
    } else {
        receipt.publication_state
    };
    let targets = receipt
        .targets
        .iter()
        .map(|target| {
            serde_json::json!({
                "target_id": target.target_id,
                "target_label": target.target_label,
                "writer_id": target.writer_id,
                // Nanosecond frontiers exceed JavaScript's exact integer
                // range. The decimal string is the exact receipt coordinate.
                "frontier": target.frontier.to_string(),
            })
        })
        .collect::<Vec<_>>();
    let capture_error = if let Some(error) = binding_error.as_deref() {
        Some(error)
    } else if matches!(capture_state, CloudCaptureState::Failed) {
        receipt.error.as_deref()
    } else {
        None
    };
    let capture = serde_json::json!({
        "state": capture_state.as_str(),
        "durable": matches!(capture_state, CloudCaptureState::Durable),
        "mutation_uuid": receipt.mutation_uuid,
        "error": capture_error,
    });
    let publication = serde_json::json!({
        "state": publication_state.as_str(),
        "published": matches!(publication_state, CloudPublicationState::Published),
        "mutation_uuid": receipt.mutation_uuid,
        "targets": targets,
        "error": if let Some(error) = binding_error.as_deref() {
            Some(error)
        } else if missing_coordinate {
            Some("exact cloud publication state has no complete target/writer/frontier coordinate")
        } else if matches!(publication_state, CloudPublicationState::Failed) {
            receipt.error.as_deref()
        } else {
            None
        },
    });
    (capture, publication)
}

pub(in super::super) fn attach_cloud_receipt(
    response: &mut Value,
    mutation_id: &str,
    receipt: Option<&CloudMutationReceipt>,
) {
    let Some(receipt) = receipt else {
        return;
    };
    let (capture, publication) = cloud_receipt_json(receipt, mutation_id);
    response["local_committed"] = Value::Bool(true);
    response["cloud_capture"] = capture;
    response["cloud_publication"] = publication;
}

pub(in super::super) fn attach_batch_cloud_capture(
    response: &mut Value,
    mutation_ids: &[String],
    receipt: Option<&CloudMutationReceipt>,
) {
    response["local_committed"] = Value::Bool(true);
    let (state, error): (CloudCaptureState, Option<String>) = match receipt {
        Some(receipt) => {
            let error = if matches!(receipt.capture_state, CloudCaptureState::Failed) {
                receipt.error.clone()
            } else {
                None
            };
            (receipt.capture_state, error)
        }
        None => (
            CloudCaptureState::Failed,
            Some("durable batch cloud capture returned no receipt".to_string()),
        ),
    };
    response["cloud_capture"] = serde_json::json!({
        "state": state.as_str(),
        "durable": matches!(state, CloudCaptureState::Durable),
        "mutation_ids": mutation_ids,
        "error": error,
    });
}

/// Request-shape gate for `must_exist`, mirroring
/// `Mutation::reject_illegal_must_exist` in the core crate.
///
/// `Update` accepts the flag: it is the opt-in that turns today's silent
/// upsert into a loud miss, so a narrow update cannot mint a row that no wide
/// reader can see. `Create` stays refused — it is an upsert by contract, and
/// `expected: Absent` is the create-if-absent precondition.
pub(in super::super) fn reject_illegal_must_exist(
    mutation_type: MutationType,
    must_exist: Option<bool>,
) -> Result<(), HostError> {
    match (mutation_type, must_exist) {
        (MutationType::Create, Some(_)) => Err(HostError::new(
            400,
            "must_exist is only valid on delete or update".to_string(),
        )),
        (MutationType::Purge, Some(false)) => Err(HostError::new(
            400,
            "purge cannot set must_exist: false".to_string(),
        )),
        _ => Ok(()),
    }
}

pub(in super::super) async fn resolve_delete_prefix_keys<H: HostNode>(
    host: &H,
    schema: String,
    hash: String,
    prefix: String,
    ctx: &AccessContext,
) -> Result<Vec<KeyValue>, HostError> {
    let query = Query::new_with_filter(
        schema,
        Vec::new(),
        Some(HashRangeFilter::HashRangePrefix { hash, prefix }),
    );
    let rows = host
        .fold_db()
        .query_executor()
        .query_with_access(query, ctx)
        .await
        .map_err(HostError::from)?;
    let mut keys = HashSet::new();
    for fields in rows.values() {
        keys.extend(fields.keys().cloned());
    }
    let mut keys: Vec<_> = keys.into_iter().collect();
    keys.sort_by(KeyValue::cmp_page_order);
    Ok(keys)
}
