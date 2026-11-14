//! `lastdb share queries save` and `lastdb share queries reconcile`.

use std::path::{Path, PathBuf};

use lastdb_node::saved_query::MAX_STORED_TEXT;
use serde_json::{json, Value};

use crate::ShareQueriesCommand;

pub(super) fn run(data_dir: Option<PathBuf>, action: ShareQueriesCommand) -> Result<(), String> {
    match action {
        ShareQueriesCommand::Save {
            id,
            app,
            source,
            destination,
            spec,
            spec_version,
            query,
        } => {
            let definition_json = read_query(&query)?;
            let (_home, socket) = crate::resolve_client_home_and_socket(data_dir)?;
            crate::require_reachable(&socket)?;
            let body = json!({
                "id": id,
                "app": app,
                "source": source,
                "destination": destination,
                "spec": spec,
                "spec_version": spec_version,
                "definition_json": definition_json,
            });
            let raw = crate::post_json_admin(&socket, "/api/saved-queries/save", &body)?;
            let reply = parse_route_reply(&raw, "/api/saved-queries/save")?;
            let saved = reply
                .get("saved")
                .ok_or("saved query response missing saved receipt")?;
            println!(
                "{}",
                json!({
                    "id": saved["id"],
                    "source": saved["source"],
                    "destination": saved["destination"],
                    "spec_name": saved["spec_name"],
                    "spec_version": saved["spec_version"],
                })
            );
        }
        ShareQueriesCommand::Reconcile { id, source } => {
            let (_home, socket) = crate::resolve_client_home_and_socket(data_dir)?;
            crate::require_reachable(&socket)?;
            let body = json!({ "id": id, "source": source });
            let raw = crate::post_json_admin(&socket, "/api/saved-queries/reconcile", &body)?;
            let reply = parse_route_reply(&raw, "/api/saved-queries/reconcile")?;
            let receipt = reply
                .get("receipt")
                .ok_or("saved query response missing reconcile receipt")?;
            println!(
                "{}",
                json!({
                    "added": receipt["added"],
                    "updated": receipt["updated"],
                    "removed": receipt["removed"],
                    "unchanged": receipt["unchanged"],
                })
            );
        }
    }
    Ok(())
}

fn read_query(path: &Path) -> Result<String, String> {
    let bytes = std::fs::read(path).map_err(|_| "saved query file is unreadable")?;
    if bytes.len() > MAX_STORED_TEXT {
        return Err("saved query definition is too large".into());
    }
    String::from_utf8(bytes).map_err(|_| "saved query file is not UTF-8".into())
}

fn parse_route_reply(raw: &str, label: &str) -> Result<Value, String> {
    match crate::parse_json_response(raw, label) {
        Ok(value) => Ok(value),
        Err(fallback) => Err(extract_error_message(raw).unwrap_or(fallback)),
    }
}

fn extract_error_message(raw: &str) -> Option<String> {
    let (_, body) = raw.split_once("\r\n\r\n")?;
    let val: Value = serde_json::from_str(body.trim()).ok()?;
    val.get("error").and_then(Value::as_str).map(str::to_owned)
}
