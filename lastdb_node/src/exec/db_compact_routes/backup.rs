use super::*;

/// Explicit local native data backup. Credentials stay in the original node home.
pub(in crate::exec) async fn execute(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "the native backup route is owner-only", ctx);
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        destination: std::path::PathBuf,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(body) => body,
        Err(error) => {
            return error_response(400, &format!("invalid native backup body: {error}"), ctx)
        }
    };
    if !body.destination.is_absolute() {
        return error_response(400, "backup destination must be absolute", ctx);
    }
    let Some(runtime) = host.db.db_ops().namespaced_store().disk_pack_runtime() else {
        return error_response(400, "this route requires a native disk home", ctx);
    };
    match runtime.backup_native(body.destination).await {
        Ok(receipt) => json_ok(&envelope(
            &serde_json::json!({"native_backup":{
            "complete":true,"generation":receipt.generation,"frontier":receipt.frontier,
            "files":receipt.files,"bytes":receipt.bytes,"includes_identity":false}}),
            ctx.user_id.as_str(),
        )),
        Err(error) => {
            let status = if error.is_storage_full() {
                507
            } else if matches!(
                error,
                fold_db::storage::StorageError::WriteResourceLimit { .. }
            ) {
                503
            } else {
                500
            };
            error_response(status, &format!("native local backup failed: {error}"), ctx)
        }
    }
}
