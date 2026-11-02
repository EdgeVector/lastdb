//! Publish the clean checkout and the running node's exact app interface.

use super::*;
use app_identity_crypto::{canonicalize, sign};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lastdb_node::{app_publish, app_registry_index as registry};
use serde_json::{json, Value};
use std::process::Command;

pub(crate) struct InterfacePublish {
    pub manifest: PathBuf,
    pub environment: Option<String>,
    pub registry_url: Option<String>,
    pub channel: Option<String>,
    pub key_file: Option<PathBuf>,
    pub api_key: Option<String>,
}

fn git(root: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!("git {} failed", args.join(" ")));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn signing_key(
    home: &Path,
    path: Option<PathBuf>,
) -> Result<app_identity_crypto::SigningKey, String> {
    let path = resolve_key_file(home, path);
    if let Some(slug) = path.to_str().and_then(|v| v.strip_prefix("lastsecrets://")) {
        let output = Command::new("lastsecrets")
            .args(["get", slug])
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err("LastSecrets publisher key lookup failed".into());
        }
        let text = String::from_utf8(output.stdout).map_err(|_| "publisher key is not UTF-8")?;
        let bytes = BASE64
            .decode(text.trim())
            .map_err(|_| "publisher key is not base64")?;
        let secret: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "publisher key must be 32 bytes")?;
        return Ok(app_identity_crypto::SigningKey::from_bytes(&secret));
    }
    app_publish::load_dev_key(&path)
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
    let version = manifest
        .get("version")
        .and_then(Value::as_str)
        .ok_or("manifest requires version")?;
    let source = git(&root, &["remote", "get-url", "origin"])?;
    if !registry::valid_source(&source) {
        return Err("origin must be a public https://github.com/owner/repo.git URL".into());
    }
    let sha = git(&root, &["rev-parse", "HEAD"])?;
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let (build, _, versions) = registry::running_node(&socket);
    let channel = input
        .channel
        .as_deref()
        .unwrap_or(registry::DEFAULT_CHANNEL);
    registry::validate_channel(channel)?;
    let mut payload = json!({"app_id": app_id, "app_version": version, "source": source, "sha": sha,
        "channel": channel, "api_version": versions.api_version, "lastdb_build": build});
    if manifest.get("outbox_format_version").is_some() {
        payload["outbox_format_version"] = json!(versions.outbox_format_version);
    }
    let key = signing_key(home, input.key_file.clone())?;
    let signature = sign(&key, &canonicalize(&payload).map_err(|e| e.to_string())?);
    payload["publisher_key"] = json!(BASE64.encode(key.verifying_key().as_bytes()));
    payload["publisher_sig"] = json!(BASE64.encode(signature));
    if let Some(description) = manifest
        .get("description")
        .or_else(|| manifest.pointer("/metadata/description"))
    {
        payload["description"] = description.clone();
    }
    Ok(payload)
}

pub(crate) fn publish_interface(home: &Path, input: InterfacePublish) -> Result<(), String> {
    let payload = publish_payload(home, &input)?;
    let url = input
        .registry_url
        .or_else(|| std::env::var("LASTDB_APP_REGISTRY_URL").ok())
        .unwrap_or_else(|| {
            folddb_profile::endpoints::app_registry_url_for(
                if input.environment.as_deref() == Some("dev") {
                    folddb_profile::endpoints::Environment::Dev
                } else {
                    folddb_profile::endpoints::Environment::Prod
                },
            )
            .to_string()
        });
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
    let api_key = require_dev_api_key(input.api_key)?;
    let response = block_on_app(async {
        // trace-egress: propagate (explicit registry publish; bearer never logged)
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
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
    if response.get("status").and_then(Value::as_str) != Some("listed") {
        return Err("registry did not list the app".into());
    }
    Ok(())
}
