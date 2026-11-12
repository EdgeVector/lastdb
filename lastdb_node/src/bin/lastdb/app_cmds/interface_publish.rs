//! Publish the clean checkout and the running node's exact app interface.

use super::*;
use app_identity_crypto::{canonicalize, sign};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use fold_db::schema::types::DeclarativeSchemaDefinition;
use lastdb_node::exec::normalize_app_manifest_schema;
use lastdb_node::{app_publish, app_registry_index as registry};
use serde_json::{json, Value};
use std::process::Command;

const NODE_SCHEMAS_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct InterfacePublish {
    pub manifest: PathBuf,
    pub environment: Option<String>,
    pub registry_url: Option<String>,
    pub channel: Option<String>,
    pub key_file: Option<PathBuf>,
    pub api_key: Option<String>,
    pub prepare: Option<PathBuf>,
}

struct SchemaIdentity {
    name: String,
    descriptive_name: Option<String>,
    identity_hash: Option<String>,
}

fn listed_schemas(manifest: &Value, app_id: &str) -> Result<Vec<SchemaIdentity>, String> {
    let schemas = manifest
        .get("schemas")
        .ok_or("manifest must list the schemas the app declares in `schemas` (an empty list when it declares none)")?
        .as_array()
        .ok_or("manifest `schemas` must be a list of schema declarations")?;
    let untyped: Vec<String> = schemas
        .iter()
        .flat_map(|s| {
            let name = s.get("name").and_then(Value::as_str).unwrap_or("?");
            let typed = s.get("field_types").and_then(Value::as_object);
            s.get("fields")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(move |f| !typed.is_some_and(|m| m.contains_key(*f)))
                .map(move |f| format!("{name}.{f}"))
        })
        .collect();
    if !untyped.is_empty() {
        return Err(format!(
            "every schema field needs a type in field_types; these fields have none: {}",
            untyped.join(", ")
        ));
    }
    schemas
        .iter()
        .map(|entry| {
            let label = entry
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("<unnamed>");
            let mut entry = entry.clone();
            if let Some(obj) = entry.as_object_mut() {
                obj.remove("retention");
            }
            let raw: DeclarativeSchemaDefinition = serde_json::from_value(entry).map_err(|e| {
                format!("manifest schema '{label}' is not a schema declaration: {e}")
            })?;
            let s = normalize_app_manifest_schema(app_id, raw)
                .map_err(|e| format!("manifest schema '{label}' cannot be declared: {e}"))?;
            if s.identity_hash.is_none() {
                return Err(format!("manifest schema '{label}' has no identity hash"));
            }
            Ok(SchemaIdentity {
                name: s.name.clone(),
                descriptive_name: s.descriptive_name.clone(),
                identity_hash: s.identity_hash.clone(),
            })
        })
        .collect()
}

fn held_schemas(socket: &Path, app_id: &str) -> Result<Vec<SchemaIdentity>, String> {
    let req = format!(
        "GET /api/schemas HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let value = parse_json_response(
        &request_readonly_with_retry(socket, req.as_bytes(), NODE_SCHEMAS_TIMEOUT)?,
        "GET /api/schemas",
    )?;
    let rows = value
        .get("data")
        .unwrap_or(&value)
        .get("schemas")
        .and_then(Value::as_array)
        .ok_or("the node's schema list has no schemas array")?;
    let text = |row: &Value, k: &str| row.get(k).and_then(Value::as_str).map(str::to_string);
    Ok(rows
        .iter()
        .filter(|row| row.get("owner_app_id").and_then(Value::as_str) == Some(app_id))
        .map(|row| SchemaIdentity {
            name: text(row, "name").unwrap_or_default(),
            descriptive_name: text(row, "descriptive_name"),
            identity_hash: text(row, "identity_hash"),
        })
        .collect())
}

fn require_held_schemas_listed(
    socket: &Path,
    app_id: &str,
    listed: &[SchemaIdentity],
) -> Result<(), String> {
    let held = held_schemas(socket, app_id).map_err(|e| {
        format!(
            "publish reads the running node at {} to check that the manifest lists every schema app '{app_id}' declares: {e}",
            socket.display()
        )
    })?;
    let unlisted: Vec<String> = held
        .iter()
        .filter_map(|h| {
            let hid = h.identity_hash.as_deref();
            let rel: Vec<_> = listed
                .iter()
                .filter(|s| {
                    s.name == h.name
                        || (hid.is_some() && s.identity_hash.as_deref() == hid)
                        || (h.descriptive_name.is_some()
                            && s.descriptive_name == h.descriptive_name)
                })
                .collect();
            if rel
                .iter()
                .any(|s| hid.is_some() && s.identity_hash.as_deref() == hid)
            {
                return None;
            }
            let shown = hid.unwrap_or("none");
            Some(rel.first().map_or_else(
                || format!("{} (identity {shown}) is not listed", h.name),
                |s| {
                    format!(
                        "{} has identity {shown} on the node, but the manifest's {} has identity {}",
                        h.name,
                        s.name,
                        s.identity_hash.as_deref().unwrap_or("none")
                    )
                },
            ))
        })
        .collect();
    unlisted.is_empty().then_some(()).ok_or_else(|| {
        format!(
            "the manifest must list every schema app '{app_id}' declares, exactly as declared; the running node holds: {}",
            unlisted.join("; ")
        )
    })
}

fn git(root: &Path, args: &[&str]) -> Result<String, String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .ok_or_else(|| format!("git {} failed", args.join(" ")))
}

fn publish_payload(home: &Path, input: &InterfacePublish) -> Result<Value, String> {
    let manifest_path = input.manifest.canonicalize().map_err(|e| e.to_string())?;
    let parent = manifest_path.parent().ok_or("manifest parent is missing")?;
    let root = PathBuf::from(git(parent, &["rev-parse", "--show-toplevel"])?);
    if !git(&root, &["status", "--porcelain"])?.is_empty() {
        return Err("publish requires a clean git checkout".into());
    }
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    let app_id = manifest
        .get("app_id")
        .and_then(Value::as_str)
        .ok_or("manifest requires app_id")?;
    if !registry::valid_app_id(app_id) {
        return Err("invalid app_id".into());
    }
    let listed = listed_schemas(&manifest, app_id)?;
    let version = manifest
        .get("version")
        .and_then(Value::as_str)
        .ok_or("manifest requires version")?;
    app_publish::validate_version(version)?;
    let source = git(&root, &["remote", "get-url", "origin"])?;
    if !registry::valid_source(&source) {
        return Err("origin must be a public https://github.com/owner/repo.git URL".into());
    }
    let sha = git(&root, &["rev-parse", "HEAD"])?;
    let socket = lastdb_node::lastdb_uds::uds::socket_path(&home.join("data"));
    let (build, _, versions) = registry::running_node(&socket);
    require_held_schemas_listed(&socket, app_id, &listed)?;
    let channel = input
        .channel
        .as_deref()
        .unwrap_or(registry::DEFAULT_CHANNEL);
    registry::validate_channel(channel)?;
    let mut payload = json!({
        "app_id": app_id, "app_version": version, "source": source, "sha": sha,
        "channel": channel, "api_version": versions.api_version, "lastdb_build": build
    });
    if manifest.get("outbox_format_version").is_some() {
        payload["outbox_format_version"] = json!(versions.outbox_format_version);
    }
    if let Some(aud) = manifest.get("audience") {
        if !aud
            .as_str()
            .is_some_and(|v| v == "public" || v == "private")
        {
            return Err("manifest audience must be public or private".into());
        }
        payload["audience"] = aud.clone();
    }
    if let Some(desc) = manifest
        .get("description")
        .or_else(|| manifest.pointer("/metadata/description"))
    {
        payload["description"] = desc.clone();
    }
    Ok(payload)
}

pub(crate) fn publish_interface(home: &Path, input: InterfacePublish) -> Result<(), String> {
    let mut payload = publish_payload(home, &input)?;
    if let Some(path) = input.prepare {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .and_then(|mut f| f.write_all(&serde_json::to_vec_pretty(&payload).unwrap_or_default()))
            .map_err(|e| e.to_string())?;
        println!("Validated unsigned app declaration; nothing published");
        return Ok(());
    }
    let key = app_publish::load_dev_key(
        &input
            .key_file
            .unwrap_or_else(|| app_publish::default_dev_key_path(home)),
    )?;
    let mut stmt = payload
        .as_object()
        .ok_or("invalid publish payload")?
        .clone();
    stmt.remove("description");
    let sig = sign(
        &key,
        &canonicalize(&Value::Object(stmt)).map_err(|e| e.to_string())?,
    );
    payload["publisher_key"] = json!(BASE64.encode(key.verifying_key().as_bytes()));
    payload["publisher_sig"] = json!(BASE64.encode(sig));
    let url = input
        .registry_url
        .or_else(|| std::env::var("LASTDB_APP_REGISTRY_URL").ok())
        .ok_or("publish requires --registry-url or LASTDB_APP_REGISTRY_URL")?;
    if !url.starts_with("https://") {
        return Err("registry URL must use HTTPS".into());
    }
    if input
        .environment
        .as_deref()
        .is_some_and(|env| env != "dev" && env != "prod")
    {
        return Err("--env must be dev or prod".into());
    }
    let api_key = input
        .api_key
        .or_else(|| std::env::var("EXEMEM_DEV_API_KEY").ok())
        .filter(|k| !k.trim().is_empty())
        .ok_or("no developer API key — pass --api-key or set EXEMEM_DEV_API_KEY")?;
    let response = block_on_app(async {
        // trace-egress: propagate (explicit registry publish; bearer never logged)
        reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| e.to_string())?
            .post(format!("{}/v1/apps", url.trim_end_matches('/')))
            .bearer_auth(api_key)
            .json(&payload)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<Value>()
            .await
            .map_err(|e| e.to_string())
    })?;
    print_pretty_json(&response);
    (response.get("status").and_then(Value::as_str) == Some("listed"))
        .then_some(())
        .ok_or_else(|| "registry did not list the app".into())
}
