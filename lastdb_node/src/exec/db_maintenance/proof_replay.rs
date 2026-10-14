//! Owner-socket proof verbs: export and replay of mutation-log segments
//! through a local directory. Every call passes `crate::proof_replay` first,
//! so a daemon that is not a guarded synthetic home refuses with 403.

use super::*;
use crate::proof_replay::{authorize, resolve_directory, Refusal};

/// Time allowed for post-ack capture and persist work to settle.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);

/// One proof verb at a time: two replays that read the same incorporated
/// frontier would apply the same records twice.
static EXCLUSIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DirBody {
    /// Absolute directory path.
    dir: String,
}

/// Wait for accepted writes, persist lanes and capture tasks, then flush.
async fn settle(host: &Host) -> Result<(), String> {
    host.db
        .flush_accepted_mutations(SETTLE_TIMEOUT)
        .await
        .map_err(|error| format!("accepted writes did not settle: {error}"))?;
    if !host.db.wait_for_background_tasks(SETTLE_TIMEOUT).await {
        return Err("capture tasks did not settle in time".to_string());
    }
    Ok(())
}

fn parse_dir_body(
    req: &UdsRequest,
    route: &str,
    ctx: &AccessContext,
) -> Result<DirBody, UdsResponse> {
    serde_json::from_slice(&req.body)
        .map_err(|e| error_response(400, &format!("invalid {route} body: {e}"), ctx))
}

fn refused(refusal: &Refusal, ctx: &AccessContext) -> UdsResponse {
    error_response(403, &format!("proof tooling refused: {}", refusal.0), ctx)
}

/// `POST /api/db/export-mutation-log` — seal this home's pending
/// mutation-log records and write the sealed segments under `dir`.
/// Body: `{ dir: string }`.
pub(in crate::exec) async fn execute_db_export_mutation_log_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let body = match parse_dir_body(req, "/api/db/export-mutation-log", ctx) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let engine = match authorize(host, "export-mutation-log") {
        Ok(engine) => engine,
        Err(refusal) => return refused(&refusal, ctx),
    };
    let dir = match resolve_directory(&body.dir, false) {
        Ok(dir) => dir,
        Err(refusal) => return refused(&refusal, ctx),
    };
    let _exclusive = EXCLUSIVE.lock().await;
    // Acked writes reach the mutation log through persist lanes and a post-ack
    // capture queue. Settle both, so the export sees every write the caller saw
    // acked.
    if let Err(error) = settle(host).await {
        return error_response(503, &error, ctx);
    }
    match engine.export_mutation_log_to_directory(&dir).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({
                "mutation_log_export": report,
                "dir": dir.display().to_string(),
                "proof_tooling": "synthetic home; no cloud",
            }),
            ctx.user_id.as_str(),
        )),
        // 422, not 500: the owner needs the text, and a 5xx message is hidden.
        Err(e) => error_response(422, &format!("export-mutation-log failed: {e}"), ctx),
    }
}

/// `POST /api/db/replay-mutation-log` — apply the sealed segments under
/// `dir` through the live peer-apply path. Body: `{ dir: string }`.
pub(in crate::exec) async fn execute_db_replay_mutation_log_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let body = match parse_dir_body(req, "/api/db/replay-mutation-log", ctx) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let engine = match authorize(host, "replay-mutation-log") {
        Ok(engine) => engine,
        Err(refusal) => return refused(&refusal, ctx),
    };
    let dir = match resolve_directory(&body.dir, true) {
        Ok(dir) => dir,
        Err(refusal) => return refused(&refusal, ctx),
    };
    let _exclusive = EXCLUSIVE.lock().await;
    match engine.replay_mutation_log_from_directory(&dir).await {
        Ok(report) => {
            // Replay flushes the namespaces it touched. Also let deferred
            // molecule persists finish, so a read or a status call after the
            // verb sees the final durable state.
            if let Err(error) = settle(host).await {
                return error_response(503, &error, ctx);
            }
            json_ok(&envelope(
                &serde_json::json!({
                    "mutation_log_replay": report,
                    "dir": dir.display().to_string(),
                    "proof_tooling": "synthetic home; no cloud",
                }),
                ctx.user_id.as_str(),
            ))
        }
        Err(e) => error_response(422, &format!("replay-mutation-log failed: {e}"), ctx),
    }
}
