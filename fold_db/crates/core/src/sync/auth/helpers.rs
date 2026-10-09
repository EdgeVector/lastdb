//! Request-body helpers for auth Lambda actions.

use crate::sync::org_sync::SyncTarget;

pub(crate) fn snapshot_presign_body(action: &str, snapshot_name: &str) -> serde_json::Value {
    named_presign_body(action, "snapshot_name", snapshot_name, None)
}

pub(crate) fn snapshot_confirm_body(snapshot_name: &str) -> serde_json::Value {
    serde_json::json!({
        "action": "confirm_upload",
        "snapshot_name": snapshot_name,
    })
}

pub(crate) fn log_confirm_body(seq_numbers: &[u64]) -> serde_json::Value {
    serde_json::json!({
        "action": "confirm_upload",
        "seq_numbers": seq_numbers,
    })
}

fn with_target_scope(mut body: serde_json::Value, target: &SyncTarget) -> serde_json::Value {
    attach_target_scope(&mut body, target);
    body
}

/// Attach the storage scope field for a [`SyncTarget`] to a
/// request body.
///
/// Personal targets attach nothing, so `resolve_scope` on the server falls
/// back to `{user_hash}`. Share targets send `share_prefix`, which the storage
/// service authorizes as either the sender's own namespace (`user_hash ==
/// sender_hash`) or, for a recipient, via a signed
/// [`SyncTarget`]-attached access grant. Org targets send `org_hash` (64 hex)
/// so objects land under `{org_hash}/log|cas|snapshots/…` and are metered as a
/// separate database under the caller's paid account.
pub(crate) fn attach_target_scope(body: &mut serde_json::Value, target: &SyncTarget) {
    if target.prefix.is_empty() {
        return;
    }
    if target.prefix.starts_with("share:") {
        body["share_prefix"] = serde_json::Value::String(target.prefix.clone());
        return;
    }
    // Org cloud prefix = 64-char hex org_hash (see OrgSyncTarget / strip_storage_prefix).
    let p = target.prefix.as_str();
    if p.len() == 64 && p.bytes().all(|b| b.is_ascii_hexdigit()) {
        body["org_hash"] = serde_json::Value::String(target.prefix.clone());
    }
}

/// Build a snapshot presign body scoped to a sync target.
pub(crate) fn snapshot_presign_body_for_target(
    action: &str,
    snapshot_name: &str,
    target: &SyncTarget,
) -> serde_json::Value {
    with_target_scope(snapshot_presign_body(action, snapshot_name), target)
}

pub(crate) fn snapshot_confirm_body_for_target(
    snapshot_name: &str,
    target: &SyncTarget,
) -> serde_json::Value {
    with_target_scope(snapshot_confirm_body(snapshot_name), target)
}

pub(crate) fn log_confirm_body_for_target(
    seq_numbers: &[u64],
    target: &SyncTarget,
) -> serde_json::Value {
    with_target_scope(log_confirm_body(seq_numbers), target)
}

/// Build a CAS presign body. `estimated_size_bytes` is meaningful for uploads
/// (quota pre-check) and is omitted otherwise.
pub(crate) fn cas_presign_body(
    action: &str,
    file_hash: &str,
    estimated_size_bytes: Option<u64>,
    app_id: Option<&str>,
) -> serde_json::Value {
    cas_presign_body_with_owner_scope(action, file_hash, estimated_size_bytes, app_id, None)
}

fn named_presign_body(
    action: &str,
    id_field: &str,
    id_value: &str,
    estimated_size_bytes: Option<u64>,
) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    body.insert(
        "action".to_string(),
        serde_json::Value::String(action.to_string()),
    );
    body.insert(
        id_field.to_string(),
        serde_json::Value::String(id_value.to_string()),
    );
    if let Some(size) = estimated_size_bytes {
        body.insert(
            "estimated_size_bytes".to_string(),
            serde_json::Value::Number(size.into()),
        );
    }
    serde_json::Value::Object(body)
}

/// Build a thumbnail (loose object) presign body. `thumb_hash` is the SHA-256
/// of the thumbnail plaintext; the sealed object is content-addressed at
/// `{scope}/thumbs/loose/sha256/{thumb_hash}` (an R2 path — read-hot, zero
/// egress; the full-size original stays on B2 `cas/`).
/// `estimated_size_bytes` is required for uploads (quota pre-check + cap).
pub(crate) fn thumb_presign_body(
    action: &str,
    thumb_hash: &str,
    estimated_size_bytes: Option<u64>,
) -> serde_json::Value {
    named_presign_body(action, "file_hash", thumb_hash, estimated_size_bytes)
}

/// Build a thumbnail-pack presign body. `pack_id` is typically the snapshot id
/// the pack was built for; the object lives at `{scope}/thumbs/packs/{pack_id}`.
pub(crate) fn thumb_pack_presign_body(
    action: &str,
    pack_id: &str,
    estimated_size_bytes: Option<u64>,
) -> serde_json::Value {
    named_presign_body(action, "pack_id", pack_id, estimated_size_bytes)
}

/// Build a CAS presign body, optionally asking the server to read from the
/// file owner's storage scope. `owner_scope` is read-only and meaningful only
/// for file downloads; uploads/deletes must stay in the caller's own scope.
pub(crate) fn cas_presign_body_with_owner_scope(
    action: &str,
    file_hash: &str,
    estimated_size_bytes: Option<u64>,
    app_id: Option<&str>,
    owner_scope: Option<&str>,
) -> serde_json::Value {
    let mut body = named_presign_body(action, "file_hash", file_hash, estimated_size_bytes);
    if let Some(app_id) = app_id {
        body["app_id"] = serde_json::Value::String(app_id.to_string());
    }
    if let Some(owner_scope) = owner_scope {
        body["owner_scope"] = serde_json::Value::String(owner_scope.to_string());
    }
    body
}
