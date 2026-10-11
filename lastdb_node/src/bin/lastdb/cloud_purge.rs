//! Owner-socket CLI for durable cloud replacement.

use super::*;
use fold_db::sync::cloud_purge::protocol::CloudAccount;

pub(crate) fn cloud_purge_resync(
    home: &Path,
    action: &str,
    operation_id: Option<&str>,
    account: Option<&CloudAccount>,
    proof_receipt: Option<&Path>,
    json: bool,
) -> Result<(), String> {
    let socket = home.join("data/folddb.sock");
    if !socket.exists() {
        return Err("the LastDB node socket is unavailable".into());
    }
    let operation_id = if action == "begin" {
        Some(
            operation_id
                .map(str::to_string)
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        )
    } else {
        operation_id.map(str::to_string)
    };
    if let Some(id) = &operation_id {
        uuid::Uuid::parse_str(id).map_err(|_| "invalid cloud purge job ID")?;
    }
    if action == "begin" {
        if let Some(id) = &operation_id {
            eprintln!("Cloud purge job: {id}");
        }
    }
    let path = proof_receipt
        .map(|path| {
            std::env::current_dir().map(|cwd| {
                if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    cwd.join(path)
                }
            })
        })
        .transpose()
        .map_err(|error| error.to_string())?;
    let body = serde_json::json!({"action":action,"operation_id":operation_id,"account":account,"proof_receipt":path});
    let response = post_json(&socket, "/api/sync/cloud-purge-resync", &body)?;
    let value: serde_json::Value = if response.starts_with("HTTP/1.1 202 ") {
        serde_json::from_str(
            response
                .split_once("\r\n\r\n")
                .ok_or("cloud purge acceptance lacks a body")?
                .1,
        )
        .map_err(|error| error.to_string())?
    } else {
        parse_json_response(&response, "cloud-purge-resync")?
    };
    if value.get("ok").and_then(serde_json::Value::as_bool) == Some(false) {
        return Err(value
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("cloud purge request failed")
            .to_string());
    }
    let report = value
        .get("job")
        .or_else(|| value.pointer("/data/job"))
        .unwrap_or(&value);
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|error| error.to_string())?
        );
    } else {
        println!(
            "Cloud purge status: {}",
            serde_json::to_string_pretty(report).map_err(|error| error.to_string())?
        );
    }
    Ok(())
}
