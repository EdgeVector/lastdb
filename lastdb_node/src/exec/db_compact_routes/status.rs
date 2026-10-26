use super::*;

/// Read the native durable boundary without advancing its normal flush schedule.
pub(in crate::exec) async fn execute(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "the native status route is owner-only", ctx);
    }
    let Some(runtime) = host.db.db_ops().namespaced_store().disk_pack_runtime() else {
        return error_response(
            400,
            "native disk status is unavailable on a legacy home",
            ctx,
        );
    };
    let retry_cache = host
        .db
        .db_ops()
        .metadata()
        .retry_cache_status()
        .await
        .unwrap_or_else(|error| serde_json::json!({"error": error.to_string()}));
    let mut status =
        serde_json::json!({"native_disk":runtime.status_native(), "retry_cache": retry_cache});
    // This existing owner route is also the isolated benchmark checkpoint.
    // Normal requests keep their prior response and perform no diagnostic work.
    if laststore::commit_timing::enabled() {
        let inventory = match inventory_requested(req) {
            Ok(inventory) => inventory,
            Err(message) => return error_response(400, message, ctx),
        };
        match runtime.diagnostics_native(inventory).await {
            Ok(Some(report)) => status["storage_diagnostics"] = serde_json::json!(report),
            Ok(None) => {}
            Err(error) => {
                return mapped_error_response("storage diagnostic failed", error.into(), ctx)
            }
        }
    }
    json_ok(&envelope(&status, ctx.user_id.as_str()))
}

fn inventory_requested(req: &UdsRequest) -> Result<bool, &'static str> {
    #[derive(Deserialize, Default)]
    struct Options {
        #[serde(default)]
        inventory: bool,
    }
    if req.body.is_empty() {
        return Ok(false);
    }
    serde_json::from_slice::<Options>(&req.body)
        .map(|options| options.inventory)
        .map_err(|_| "invalid native-status diagnostic options")
}
