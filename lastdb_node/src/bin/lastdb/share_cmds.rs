//! Open-material mint and accept over the owner socket.

use super::{
    build_post_request, parse_json_response, post_json_with_timeout, request_with_timeout,
    DatabaseTarget, ShareCommand,
};
use lastdb_node::lastdb_uds::{handler_timeout, valid_selector};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

pub(super) fn run(
    data_dir: Option<PathBuf>,
    _db: Option<&str>,
    action: ShareCommand,
) -> Result<(), String> {
    match action {
        ShareCommand::Accept { link, name } => {
            post_accept(data_dir, &json!({ "link": link, "name": name }))
        }
        ShareCommand::Link { database } => mint_link(data_dir, &database),
    }
}

fn post_accept(data_dir: Option<PathBuf>, body: &Value) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, None)?;
    let raw = post_json_with_timeout(
        &target.socket,
        &target.path("/api/databases/accept"),
        body,
        Duration::from_secs(10),
    )?;
    println!("{}", parse_json_response(&raw, "database registration")?);
    Ok(())
}

fn mint_link(data_dir: Option<PathBuf>, database: &str) -> Result<(), String> {
    if !valid_selector(database) {
        return Err("invalid database ID or name".into());
    }
    let target = DatabaseTarget::open(data_dir, Some(database))?;
    let path = target.path("/api/share/link");
    let raw = request_with_timeout(
        &target.socket,
        &build_post_request(path.as_ref(), "", &[]),
        handler_timeout(),
    )?;
    println!("{}", owner_json(&raw, "open material")?);
    Ok(())
}

fn owner_json(raw: &str, label: &str) -> Result<Value, String> {
    match parse_json_response(raw, label) {
        Ok(value) => Ok(value),
        Err(fallback) => Err(owner_error_text(raw).unwrap_or(fallback)),
    }
}

fn owner_error_text(raw: &str) -> Option<String> {
    let body = raw.split_once("\r\n\r\n")?.1.trim();
    let value: Value = serde_json::from_str(body).ok()?;
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty());
    let code = value
        .get("error")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty());
    match (code, message) {
        (Some(code), Some(message)) => Some(format!("{message} ({code})")),
        (_, Some(message)) => Some(message.to_owned()),
        (Some(code), None) => Some(code.to_owned()),
        _ => None,
    }
}
