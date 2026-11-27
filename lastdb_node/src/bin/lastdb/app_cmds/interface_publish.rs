//! Publish the clean checkout and the running node's exact app interface.

use super::owner_app::record_published_location;
use super::publish_credentials::{self, RegistryAuth};
use super::*;
use crate::app_registry_index as registry;
use app_identity_crypto::{canonicalize, sign};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use fold_db::schema::types::DeclarativeSchemaDefinition;
use lastdb_node::exec::normalize_app_manifest_schema;
use schema_types::expand_manifest_schema;
use serde_json::{json, Value};
use std::process::Command;
use std::time::Duration;

#[path = "schema_push.rs"]
mod schema_push;

type SchemaIdentity = (String, Option<String>, Option<String>);

fn listed_schemas(manifest: &Value, app_id: &str) -> Result<Vec<SchemaIdentity>, String> {
    let schemas = manifest
        .get("schemas")
        .ok_or("manifest must list the schemas the app declares in `schemas` (an empty list when it declares none)")?
        .as_array()
        .ok_or("manifest `schemas` must be a list of schema declarations")?;
    let untyped: Vec<String> = schemas
        .iter()
        .flat_map(|s| {
            let name = s["name"].as_str().unwrap_or("?");
            let typed = s["field_types"].as_object();
            s["fields"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(move |v| {
                    let f = v.as_str()?;
                    (!typed.is_some_and(|m| m.contains_key(f))).then(|| format!("{name}.{f}"))
                })
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
            let label = entry["name"].as_str().unwrap_or("<unnamed>");
            let mut entry = entry.clone();
            if let Some(obj) = entry.as_object_mut() {
                obj.remove("retention");
            }
            expand_manifest_schema(&mut entry)?;
            let raw: DeclarativeSchemaDefinition = serde_json::from_value(entry).map_err(|e| {
                format!("manifest schema '{label}' is not a schema declaration: {e}")
            })?;
            let s = normalize_app_manifest_schema(app_id, raw)
                .map_err(|e| format!("manifest schema '{label}' cannot be declared: {e}"))?;
            if s.identity_hash.is_none() {
                return Err(format!("manifest schema '{label}' has no identity hash"));
            }
            Ok((
                s.name.clone(),
                s.descriptive_name.clone(),
                s.identity_hash.clone(),
            ))
        })
        .collect()
}

fn require_held_schemas_listed(
    socket: &Path,
    app_id: &str,
    listed: &[SchemaIdentity],
) -> Result<(), String> {
    let held = get_path_retry(socket, "/api/schemas", Duration::from_secs(30))
        .and_then(|raw| parse_json_response(&raw, "GET /api/schemas"))
        .and_then(|v| {
            let rows = v.get("data").unwrap_or(&v)["schemas"]
                .as_array()
                .ok_or_else(|| "the node's schema list has no schemas array".to_string())?;
            let text = |r: &Value, k: &str| r[k].as_str().map(str::to_string);
            Ok(rows
                .iter()
                .filter(|r| r["owner_app_id"].as_str() == Some(app_id))
                .map(|r| {
                    (
                        text(r, "name").unwrap_or_default(),
                        text(r, "descriptive_name"),
                        text(r, "identity_hash"),
                    )
                })
                .collect::<Vec<_>>())
        })
        .map_err(|e| {
            format!(
                "publish reads the running node at {} to check that the manifest lists every schema app '{app_id}' declares: {e}",
                socket.display()
            )
        })?;
    let unlisted: Vec<String> = held
        .iter()
        .filter_map(|(h_name, h_desc, h_id)| {
            let hid = h_id.as_deref();
            let rel: Vec<_> = listed
                .iter()
                .filter(|(n, d, id)| n == h_name || (hid.is_some() && id.as_deref() == hid) || (h_desc.is_some() && d == h_desc))
                .collect();
            if rel.iter().any(|(_, _, id)| hid.is_some() && id.as_deref() == hid) {
                return None;
            }
            let shown = hid.unwrap_or("none");
            Some(rel.first().map_or_else(
                || format!("{h_name} (identity {shown}) is not listed"),
                |(n, _, id)| {
                    format!(
                        "{h_name} has identity {shown} on the node, but the manifest's {n} has identity {}",
                        id.as_deref().unwrap_or("none")
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

enum ManifestSide {
    All,
    Names(Vec<String>),
}

/// Turn the manifest's access block into the listing's: each schema name gains
/// the definition ID of the version the running node holds, which is the one
/// the developer built against. `"*"` is allowed at every app API version and
/// stays `"*"`.
fn declared_access(socket: &Path, decl: &Value) -> Result<Value, String> {
    let read = manifest_side(decl, "read")?;
    let write = manifest_side(decl, "write")?;
    let extensions = manifest_list(decl, "extensions")?;
    let rows = schema_rows(socket)?;
    let access = json!({
        "read": side_value(&read, &rows)?,
        "write": side_value(&write, &rows)?,
        "extensions": extensions,
    });
    let parsed: app_identity_crypto::Access =
        serde_json::from_value(access.clone()).map_err(|e| format!("manifest `access`: {e}"))?;
    if parsed.well_formed() {
        Ok(access)
    } else {
        Err("manifest `access` has an invalid schema name or extension name".into())
    }
}

fn manifest_side(decl: &Value, key: &str) -> Result<ManifestSide, String> {
    let Some(value) = decl.get(key) else {
        return Err(format!(
            "manifest `access.{key}` must be a list of schema names or \"*\""
        ));
    };
    if value.as_str() == Some(app_identity_crypto::ALL_SCHEMAS) {
        return Ok(ManifestSide::All);
    }
    let Some(items) = value.as_array() else {
        return Err(format!(
            "manifest `access.{key}` must be a list of schema names or \"*\""
        ));
    };
    let names = items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("manifest `access.{key}` entries must be strings"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ManifestSide::Names(names))
}

fn manifest_list(decl: &Value, key: &str) -> Result<Vec<String>, String> {
    decl.get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("manifest `access.{key}` must be a list"))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("manifest `access.{key}` entries must be strings"))
        })
        .collect()
}

fn schema_rows(socket: &Path) -> Result<Vec<Value>, String> {
    let path = "/api/schemas?include_definition_ids=true";
    let held = get_path_retry(socket, path, Duration::from_secs(30))
        .and_then(|raw| parse_json_response(&raw, "GET /api/schemas"))?;
    Ok(held.get("data").unwrap_or(&held)["schemas"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}

fn side_value(side: &ManifestSide, rows: &[Value]) -> Result<Value, String> {
    let ManifestSide::Names(names) = side else {
        return Ok(json!(app_identity_crypto::ALL_SCHEMAS));
    };
    names
        .iter()
        .map(|name| {
            rows.iter()
                .find(|row| row["name"].as_str() == Some(name.as_str()))
                .and_then(|row| row["definition_id"].as_str())
                .map(|id| (name.clone(), json!(id)))
                .ok_or_else(|| {
                    format!(
                        "access names schema '{name}', but the running node holds no versioned definition of it"
                    )
                })
        })
        .collect::<Result<serde_json::Map<String, Value>, String>>()
        .map(Value::Object)
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

fn publish_payload(home: &Path, input: &PublishArgs) -> Result<Value, String> {
    let path = input.manifest.canonicalize().map_err(|e| e.to_string())?;
    let root = PathBuf::from(git(
        path.parent().ok_or("manifest parent is missing")?,
        &["rev-parse", "--show-toplevel"],
    )?);
    if !git(&root, &["status", "--porcelain"])?.is_empty() {
        return Err("publish requires a clean git checkout".into());
    }
    let m: Value = serde_json::from_slice(&std::fs::read(&path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let app_id = m["app_id"].as_str().ok_or("manifest requires app_id")?;
    if !registry::valid_app_id(app_id) {
        return Err("invalid app_id".into());
    }
    let listed = listed_schemas(&m, app_id)?;
    let ver = m["version"].as_str().ok_or("manifest requires version")?;
    if ver.trim() != ver || ver.is_empty() {
        return Err(
            "version must be a non-empty SemVer string without surrounding whitespace".into(),
        );
    }
    semver::Version::parse(ver).map_err(|e| format!("version must be valid SemVer: {e}"))?;
    let source = git(&root, &["remote", "get-url", "origin"])?;
    if !registry::valid_source(&source) {
        return Err("origin must be a public https://github.com/owner/repo.git URL".into());
    }
    let sha = git(&root, &["rev-parse", "HEAD"])?;
    let (build, _, api_version, outbox, _) = registry::running_node(home)?;
    let sock = lastdb_node::lastdb_uds::socket_path(&home.join("data"));
    require_held_schemas_listed(&sock, app_id, &listed)?;
    let channel = input
        .channel
        .as_deref()
        .unwrap_or(registry::DEFAULT_CHANNEL);
    registry::validate_channel(channel)?;
    let mut payload = json!({
        "app_id": app_id, "app_version": ver, "source": source, "sha": sha,
        "channel": channel, "api_version": api_version, "lastdb_build": build
    });
    if let Some(decl) = m.get("access") {
        payload["access"] = declared_access(&sock, decl)?;
    }
    if m.get("outbox_format_version").is_some() {
        payload["outbox_format_version"] = json!(outbox);
    }
    if let Some(aud) = m.get("audience") {
        if !aud
            .as_str()
            .is_some_and(|v| v == "public" || v == "private")
        {
            return Err("manifest audience must be public or private".into());
        }
        payload["audience"] = aud.clone();
    }
    if let Some(d) = m
        .get("description")
        .or_else(|| m.pointer("/metadata/description"))
    {
        payload["description"] = d.clone();
    }
    Ok(payload)
}

pub(super) fn publish_interface(home: &Path, input: &PublishArgs) -> Result<(), String> {
    let mut payload = publish_payload(home, input)?;
    if let Some(path) = &input.prepare {
        let pretty = serde_json::to_vec_pretty(&payload).unwrap_or_default();
        crate::write_new_private_file(path, &pretty, "prepared app declaration")?;
        println!("Validated unsigned app declaration; nothing published");
        return Ok(());
    }
    let auth = RegistryAuth {
        env: input.env.as_deref(),
        api_key: input.api_key.as_deref(),
        registry_url: input.registry_url.as_deref(),
    };
    let target = publish_credentials::resolve_target(home, &auth)?;
    let key = publish_credentials::publisher_signing_key(
        home,
        input.key_file.as_deref(),
        &auth,
        target.native_registry,
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
    let (mut response, private_note) =
        publish_credentials::with_registry_credential(home, &auth, &target, |api_key, source| {
            let mut resp = crate::block_on(
                "app publish",
                publish_credentials::post_publish(&target.url, api_key, source, &payload),
            )?;
            let note = if resp["status"].as_str() == Some("listed") {
                record_published_location(&target, api_key, source, &payload, &mut resp)?
            } else {
                None
            };
            Ok((resp, note))
        })?;
    let note = schema_push::register(&mut schema_push::Request {
        home,
        env: target.target_env,
        input,
        key: &key,
        payload: &payload,
        response: &mut response,
    });
    print_pretty_json(&response);
    if !input.json {
        if let Some(line) = private_note {
            println!("{line}");
        }
        if let Some(line) = note {
            println!("{line}");
        }
    }
    (response["status"].as_str() == Some("listed"))
        .then_some(())
        .ok_or_else(|| "registry did not list the app".into())
}
