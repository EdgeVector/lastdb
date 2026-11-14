//! Developer API key management (`lastdb cloud developer-key {create,list,revoke}`).
use super::account::{bool_field, ensure_ok, exchange_json, str_field, CloudSession};
use super::block_on_cloud;
use crate::app_cmds::print_pretty_json;
use crate::cli_defs::CloudDeveloperKeyCommand;
use lastdb_node::cloud::{
    developer_key_prefix, is_valid_developer_key, is_valid_key_prefix,
    remove_saved_developer_key_if_prefix, save_developer_key,
};
use reqwest::Method;
use serde_json::{json, Value};
use std::path::Path;
use zeroize::Zeroizing;

pub(super) fn run_developer_key(
    home: &Path,
    session: &CloudSession,
    action: &CloudDeveloperKeyCommand,
) -> Result<(), String> {
    match action {
        CloudDeveloperKeyCommand::Create { save, json } => {
            dev_key_create(home, session, *save, *json)
        }
        CloudDeveloperKeyCommand::List { json } => dev_key_list(session, *json),
        CloudDeveloperKeyCommand::Revoke { prefix, json } => {
            dev_key_revoke(home, session, prefix, *json)
        }
    }
}

fn dev_key_create(
    home: &Path,
    session: &CloudSession,
    save: bool,
    json: bool,
) -> Result<(), String> {
    let empty = json!({});
    let (status, body) = block_on_cloud(
        "developer key create",
        exchange_json(
            session,
            Method::POST,
            "/api/developer/api-keys",
            Some(&empty),
        ),
    )?;
    if status == 403 && body.get("code").and_then(Value::as_str) == Some("PAID_ACCOUNT_REQUIRED") {
        return Err(
            "active paid cloud subscription required for developer API keys — run 'lastdb cloud payment checkout' first".into(),
        );
    }
    ensure_ok(status, &body, "developer key create")?;
    let raw_key = Zeroizing::new(str_field(&body, "key", "developer key create")?);
    let prefix = str_field(&body, "prefix", "developer key create")?;
    let created_at = body
        .get("created_at")
        .and_then(Value::as_i64)
        .ok_or("developer key create response missing created_at")?;
    if !is_valid_developer_key(&raw_key)
        || developer_key_prefix(&raw_key).as_deref() != Some(prefix.as_str())
    {
        return Err("developer key create returned an invalid API key or mismatched prefix".into());
    }
    if save {
        let key_file = save_developer_key(home, &raw_key)?;
        if json {
            print_pretty_json(&json!({
                "ok": true, "saved": true, "key_file": key_file, "prefix": prefix,
                "created_at": created_at, "environment": session.env.as_str(),
            }));
        } else {
            println!(
                "Saved developer API key {prefix} to {} (created_at={created_at})",
                key_file.display()
            );
        }
        return Ok(());
    }
    if json {
        print_pretty_json(&json!({
            "ok": true, "saved": false, "key": raw_key.as_str(), "prefix": prefix,
            "created_at": created_at, "environment": session.env.as_str(),
        }));
    } else {
        println!("Key:        {}", raw_key.as_str());
        println!("Prefix:     {prefix}");
        println!("Created at: {created_at}");
    }
    Ok(())
}

fn dev_key_list(session: &CloudSession, json: bool) -> Result<(), String> {
    let (status, body) = block_on_cloud(
        "developer key list",
        exchange_json(session, Method::GET, "/api/developer/api-keys", None),
    )?;
    ensure_ok(status, &body, "developer key list")?;
    let rows = body
        .get("keys")
        .and_then(Value::as_array)
        .ok_or("developer key list response missing keys array")?;
    let mut keys = Vec::with_capacity(rows.len());
    for row in rows {
        let prefix = str_field(row, "prefix", "developer key list")?;
        if !is_valid_key_prefix(&prefix) {
            return Err("developer key list returned an invalid key prefix".into());
        }
        let created_at = row
            .get("created_at")
            .and_then(Value::as_i64)
            .ok_or("developer key list row missing created_at")?;
        let last_used_at = match row.get("last_used_at") {
            None | Some(Value::Null) => None,
            Some(v) => Some(
                v.as_i64()
                    .ok_or("developer key list row has invalid last_used_at")?,
            ),
        };
        let is_active = bool_field(row, "is_active", "developer key list")?;
        keys.push(json!({
            "prefix": prefix, "created_at": created_at,
            "last_used_at": last_used_at, "is_active": is_active,
        }));
    }
    if json {
        print_pretty_json(&json!({
            "ok": true, "environment": session.env.as_str(), "keys": keys,
        }));
        return Ok(());
    }
    if keys.is_empty() {
        println!("No developer API keys.");
        return Ok(());
    }
    for item in &keys {
        let prefix = item["prefix"].as_str().unwrap_or("");
        let state = if item["is_active"].as_bool() == Some(true) {
            "active"
        } else {
            "revoked"
        };
        let created = item["created_at"].as_i64().unwrap_or(0);
        let used = item["last_used_at"]
            .as_i64()
            .map_or_else(|| "never".to_owned(), |ts| ts.to_string());
        println!("{prefix}\t{state}\tcreated_at={created}\tlast_used_at={used}");
    }
    Ok(())
}

fn dev_key_revoke(
    home: &Path,
    session: &CloudSession,
    raw_prefix: &str,
    json: bool,
) -> Result<(), String> {
    let prefix = raw_prefix.trim();
    if !is_valid_key_prefix(prefix) {
        return Err(
            "invalid developer key prefix — expected 'em_<10 hex>...' from 'lastdb cloud developer-key list'".into(),
        );
    }
    if developer_key_prefix(&session.api_key).as_deref() == Some(prefix) {
        return Err("refusing to revoke this home's active cloud backup credential".into());
    }
    let payload = json!({ "prefix": prefix });
    let (status, body) = block_on_cloud(
        "developer key revoke",
        exchange_json(
            session,
            Method::DELETE,
            "/api/developer/api-keys",
            Some(&payload),
        ),
    )?;
    ensure_ok(status, &body, "developer key revoke")?;
    let removed_saved_key = remove_saved_developer_key_if_prefix(home, prefix)?;
    if json {
        print_pretty_json(&json!({
            "ok": true, "prefix": prefix, "revoked": true,
            "removed_saved_key": removed_saved_key, "environment": session.env.as_str(),
        }));
    } else if removed_saved_key {
        println!("Revoked developer API key {prefix} and removed saved developer-api-key file.");
    } else {
        println!("Revoked developer API key {prefix}.");
    }
    Ok(())
}
