//! Themed module split from the parent.

use super::*;

/// `lastdb app index verify|trust-key`.
pub(crate) fn app_index_command(cmd: AppIndexCommand) -> Result<(), String> {
    match cmd {
        AppIndexCommand::Verify {
            index,
            sig,
            trust_key,
            json,
        } => app_index_verify(&index, sig, trust_key.as_deref(), json),
        AppIndexCommand::TrustKey => {
            let value = lastdb_node::app_registry_index::REGISTRY_LIST_PUBKEY_B64;
            let key =
                app_identity_crypto::verifying_key_from_base64(value).map_err(|e| e.to_string())?;
            println!(
                "registry list: {} {value}",
                app_identity_crypto::key_id(&key)
            );
            Ok(())
        }
    }
}

/// `<index>.sig`, the default signature path beside an index file.
fn default_sig_path(index: &Path) -> PathBuf {
    let mut p = index.to_path_buf().into_os_string();
    p.push(".sig");
    PathBuf::from(p)
}

/// `lastdb app index verify`: check a list file against its signature.
fn app_index_verify(
    index: &Path,
    sig: Option<PathBuf>,
    trust_key: Option<&str>,
    json: bool,
) -> Result<(), String> {
    use lastdb_node::app_registry_index as registry_index;
    let sig = sig.unwrap_or_else(|| default_sig_path(index));
    let bytes =
        std::fs::read(index).map_err(|e| format!("failed to read {}: {e}", index.display()))?;
    let sig_bytes =
        std::fs::read(&sig).map_err(|e| format!("failed to read {}: {e}", sig.display()))?;
    let detached: registry_index::IndexSignature =
        serde_json::from_slice(&sig_bytes).map_err(|e| e.to_string())?;
    let (trust, trust_source) = registry_index::resolve_list_trust_key(trust_key)?;
    warn_trust_override(&trust_source);
    registry_index::verify_index_bytes(&trust, &bytes, &detached)?;
    let parsed: registry_index::InterfaceList =
        serde_json::from_slice(&bytes).map_err(|e| format!("invalid registry list: {e}"))?;
    let parsed = registry_index::InterfaceList::load(
        &bytes,
        &sig_bytes,
        &trust,
        &parsed.channel,
        parsed.api_version,
    )?;
    let summary = serde_json::json!({"verified": true, "index": index, "channel": parsed.channel,
        "generated_at": parsed.generated_at, "apps": parsed.apps.len(), "api_version": parsed.api_version,
        "seq": parsed.seq, "key_id": detached.key_id});
    if json {
        print_pretty_json(&summary);
    } else {
        println!("verified {}: {}", index.display(), summary);
    }
    Ok(())
}

pub(crate) fn resolve_schema_service_url(
    schema_url: Option<String>,
    env: Option<&str>,
) -> Result<String, String> {
    match (schema_url, env) {
        (Some(url), _) => Ok(url),
        (None, Some("dev") | None) => Ok(folddb_profile::endpoints::schema_service_url_for(
            folddb_profile::endpoints::Environment::Dev,
        )
        .to_string()),
        (None, Some("prod")) => Ok(folddb_profile::endpoints::schema_service_url_for(
            folddb_profile::endpoints::Environment::Prod,
        )
        .to_string()),
        (None, Some(other)) => Err(format!("unknown --env '{other}' (expected dev or prod)")),
    }
}

/// The developer API key from `--api-key` or `$EXEMEM_DEV_API_KEY`.
pub(crate) fn require_dev_api_key(api_key: Option<String>) -> Result<String, String> {
    match api_key.or_else(|| std::env::var("EXEMEM_DEV_API_KEY").ok()) {
        Some(k) if !k.trim().is_empty() => Ok(k),
        _ => Err("no developer API key — pass --api-key or set EXEMEM_DEV_API_KEY".into()),
    }
}
