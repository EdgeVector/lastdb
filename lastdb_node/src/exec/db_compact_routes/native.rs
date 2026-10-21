use super::*;
use fold_db::storage::disk_pack::{NativeMaintenanceOptions, PackRuntime};

/// The owner compact route maintains the native home.
/// Native parts share one root; a named collection must not imply narrower work.
pub(super) async fn execute(
    req: &UdsRequest,
    ctx: &AccessContext,
    runtime: std::sync::Arc<PackRuntime>,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
        collection: Option<String>,
        dry_run: Option<bool>,
        #[serde(default)]
        reclaim_only: bool,
        grace_ms: Option<u64>,
    }
    let body: Body = if req.body.is_empty() {
        Body::default()
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(body) => body,
            Err(error) => {
                return error_response(400, &format!("invalid native compact body: {error}"), ctx)
            }
        }
    };
    if body
        .collection
        .as_deref()
        .is_some_and(|name| !name.trim().is_empty())
    {
        return error_response(
            400,
            "native disk compact uses the whole home; omit collection",
            ctx,
        );
    }
    let options = NativeMaintenanceOptions {
        dry_run: body.dry_run.unwrap_or(true),
        reclaim_only: body.reclaim_only,
        grace_ms: body.grace_ms,
        report_usage: true,
        reclaim_generations: None,
    };
    match runtime.maintain_native(options).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({"compact":{"native_disk":report}}),
            ctx.user_id.as_str(),
        )),
        Err(error) => error_response(500, &format!("native disk compact failed: {error}"), ctx),
    }
}
