//! Owner registry inspection (`lastdb app show`) and post-publish private list persistence.

use super::publish_credentials::{self, RegistryAuth, RegistryTarget};
use super::{print_pretty_json, OwnerAppArgs};
use crate::app_registry_index as registry;
use lastdb_node::profile::endpoints::{
    environment_for_registry_api_url, registry_index_url_for, Environment,
};
use serde_json::{json, Value};
use std::path::Path;

pub(super) fn record_published_location(
    target: &RegistryTarget,
    api_key: &str,
    key_source: &str,
    payload: &Value,
    response: &mut Value,
) -> Result<Option<String>, String> {
    let audience = payload
        .get("audience")
        .and_then(Value::as_str)
        .unwrap_or("public");
    let Some(obj) = response.as_object_mut() else {
        return Ok(None);
    };
    obj.insert("audience".into(), json!(audience));
    let reg_env = environment_for_registry_api_url(&target.url);
    if audience != "private" {
        if let Some(env) = reg_env {
            obj.insert("index".into(), json!(registry_index_url_for(env)));
        }
        return Ok(None);
    }
    let app_id = payload["app_id"]
        .as_str()
        .ok_or("publish payload missing app_id")?;
    let owned = crate::block_on(
        "app owner read",
        publish_credentials::get_owned_app(&target.url, api_key, key_source, app_id),
    )?;
    let meta = &owned["metadata"];
    let account = meta["account"]
        .as_str()
        .ok_or("registry owner metadata missing account")?;
    let prefix = meta["private_prefix"]
        .as_str()
        .filter(|p| registry::private_lists::valid_private_prefix(p))
        .ok_or("registry owner metadata missing valid 32-hex private_prefix")?;
    if let Some(env) = reg_env {
        let locator = registry::private_lists::save(env, account, prefix)?;
        obj.insert("index".into(), json!(locator));
        return Ok(Some(format!(
            "Saved private list locator {locator} (run 'lastdb app show {app_id}' to view the private list URL)"
        )));
    }
    let path = format!("/v1/lists/private/{prefix}");
    obj.insert("private_list_path".into(), json!(path));
    Ok(Some(format!("Listed privately under {path}")))
}

pub(super) fn show_owned_app(home: &Path, args: &OwnerAppArgs) -> Result<(), String> {
    if !registry::valid_app_id(&args.app_id) {
        return Err("invalid app_id".into());
    }
    let auth = RegistryAuth {
        env: args.env.as_deref(),
        api_key: args.api_key.as_deref(),
        registry_url: args.registry_url.as_deref(),
    };
    let target = publish_credentials::resolve_target(home, &auth)?;
    let owned = publish_credentials::with_registry_credential(
        home,
        &auth,
        &target,
        |api_key, key_source| {
            crate::block_on(
                "app show",
                publish_credentials::get_owned_app(&target.url, api_key, key_source, &args.app_id),
            )
        },
    )?;
    let reg_env = environment_for_registry_api_url(&target.url);
    let summary = build_summary(&args.app_id, target.target_env, reg_env, &owned, args.save)?;
    if args.json {
        print_pretty_json(&summary);
    } else {
        print_human_summary(&summary);
    }
    Ok(())
}

fn build_summary(
    app_id: &str,
    target_env: Environment,
    reg_env: Option<Environment>,
    owned: &Value,
    save_only: bool,
) -> Result<Value, String> {
    let meta = owned
        .get("metadata")
        .and_then(Value::as_object)
        .ok_or("registry owner response missing metadata")?;
    let str_f = |k: &str| meta.get(k).and_then(Value::as_str).unwrap_or("");
    let audience = meta
        .get("audience")
        .and_then(Value::as_str)
        .unwrap_or("public");
    let status = str_f("status");
    let account = str_f("account");
    let publisher_key = str_f("publisher_key");
    let source = str_f("source");
    let mut index: Option<String> = None;
    let mut list_url: Option<String> = None;
    let mut saved = false;
    if audience == "private" {
        let prefix = meta
            .get("private_prefix")
            .and_then(Value::as_str)
            .filter(|p| registry::private_lists::valid_private_prefix(p))
            .ok_or("registry owner metadata missing valid 32-hex private_prefix")?;
        if let Some(env) = reg_env {
            index = Some(registry::private_lists::save(env, account, prefix)?);
            saved = true;
            if !save_only {
                list_url = Some(registry::private_lists::private_index_url(env, prefix));
            }
        } else if !save_only {
            list_url = Some(format!("/v1/lists/private/{prefix}"));
        }
    } else if let Some(env) = reg_env {
        let public = registry_index_url_for(env).to_owned();
        index = Some(public.clone());
        if !save_only {
            list_url = Some(public);
        }
    }
    let interfaces = clean_interfaces(owned.get("interfaces"));
    let mut out = json!({
        "app_id": app_id,
        "environment": reg_env.unwrap_or(target_env).as_str(),
        "status": status,
        "audience": audience,
        "account": account,
        "publisher_key": publisher_key,
        "source": source,
        "index": index,
        "saved": saved,
        "interfaces": interfaces,
    });
    if let (Some(url), Some(obj)) = (list_url, out.as_object_mut()) {
        obj.insert("list_url".into(), json!(url));
    }
    Ok(out)
}

fn clean_interfaces(raw: Option<&Value>) -> Vec<Value> {
    let Some(rows) = raw.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut items: Vec<Value> = rows
        .iter()
        .map(|row| {
            json!({
                "channel": row.get("channel").and_then(Value::as_str).unwrap_or(""),
                "api_version": row.get("api_version").and_then(Value::as_u64).unwrap_or(0),
                "app_version": row.get("app_version").and_then(Value::as_str).unwrap_or(""),
                "sha": row.get("sha").and_then(Value::as_str).unwrap_or(""),
                "status": row.get("status").and_then(Value::as_str).unwrap_or(""),
                "published_at": row.get("published_at").and_then(Value::as_str).unwrap_or(""),
            })
        })
        .collect();
    items.sort_by(|a, b| {
        let ca = a["channel"].as_str().unwrap_or("");
        let cb = b["channel"].as_str().unwrap_or("");
        ca.cmp(cb)
            .then_with(|| a["api_version"].as_u64().cmp(&b["api_version"].as_u64()))
    });
    items
}

fn print_human_summary(summary: &Value) {
    let s = |k: &str| summary.get(k).and_then(Value::as_str).unwrap_or("");
    println!("App:           {}", s("app_id"));
    println!("Environment:   {}", s("environment"));
    println!("Status:        {}", s("status"));
    println!("Audience:      {}", s("audience"));
    println!("Account:       {}", s("account"));
    if let Some(idx) = summary.get("index").and_then(Value::as_str) {
        let suffix = if summary["saved"].as_bool() == Some(true) {
            " (saved)"
        } else {
            ""
        };
        println!("Index:         {idx}{suffix}");
    }
    if let Some(url) = summary.get("list_url").and_then(Value::as_str) {
        println!("List URL:      {url}");
    }
    println!("Publisher key: {}", s("publisher_key"));
    if !s("source").is_empty() {
        println!("Source:        {}", s("source"));
    }
    if let Some(rows) = summary
        .get("interfaces")
        .and_then(Value::as_array)
        .filter(|r| !r.is_empty())
    {
        println!("Interfaces:");
        for r in rows {
            println!(
                "  {}\tapp API {}\t{}\t{}\t{}\t{}",
                r["channel"].as_str().unwrap_or(""),
                r["api_version"].as_u64().unwrap_or(0),
                r["app_version"].as_str().unwrap_or(""),
                r["sha"].as_str().unwrap_or(""),
                r["status"].as_str().unwrap_or(""),
                r["published_at"].as_str().unwrap_or(""),
            );
        }
    }
}
