//! Themed module split from the parent.

use super::*;

/// `lastdb app index sign|verify|trust-key`.
pub(crate) fn app_index_command(home: &Path, cmd: AppIndexCommand) -> Result<(), String> {
    match cmd {
        AppIndexCommand::Sign {
            index,
            key_file,
            out,
            json,
        } => app_index_sign(home, &index, key_file, out, json),
        AppIndexCommand::Verify {
            index,
            sig,
            trust_key,
            json,
        } => app_index_verify(&index, sig, trust_key.as_deref(), json),
        AppIndexCommand::TrustKey => {
            println!(
                "{}",
                lastdb_node::app_registry_index::RELEASE_INDEX_PUBKEY_B64
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

/// `lastdb app index sign`: sign an index file and write the signature.
fn app_index_sign(
    home: &Path,
    index: &Path,
    key_file: Option<PathBuf>,
    out: Option<PathBuf>,
    json: bool,
) -> Result<(), String> {
    use lastdb_node::app_publish;
    use lastdb_node::app_registry_index as registry_index;
    let key_path = key_file
        .or_else(|| std::env::var_os("LASTDB_REGISTRY_SIGNING_KEY").map(PathBuf::from))
        .unwrap_or_else(|| home.join("registry-index-signing.key"));
    let key = app_publish::load_dev_key(&key_path)?;
    let bytes =
        std::fs::read(index).map_err(|e| format!("failed to read {}: {e}", index.display()))?;
    registry_index::parse_index(&bytes)?;
    let signature = registry_index::sign_index_bytes(&key, &bytes);
    let out = out.unwrap_or_else(|| default_sig_path(index));
    let encoded = serde_json::to_vec_pretty(&signature)
        .map_err(|e| format!("failed to encode signature: {e}"))?;
    std::fs::write(&out, encoded).map_err(|e| format!("failed to write {}: {e}", out.display()))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "index": index,
                "sig": out,
                "key_id": signature.key_id,
                "payload_sha256": signature.payload_sha256,
            }))
            .unwrap_or_default()
        );
    } else {
        println!(
            "signed {} -> {} (key {})",
            index.display(),
            out.display(),
            signature.key_id
        );
    }
    Ok(())
}

/// `lastdb app index verify`: check an index file against its signature.
fn app_index_verify(
    index: &Path,
    sig: Option<PathBuf>,
    trust_key: Option<&str>,
    json: bool,
) -> Result<(), String> {
    use lastdb_node::app_registry_index as registry_index;
    let sig = sig.unwrap_or_else(|| default_sig_path(index));
    let (trust, trust_source) = registry_index::resolve_trust_key(trust_key)?;
    warn_trust_override(&trust_source);
    let bytes =
        std::fs::read(index).map_err(|e| format!("failed to read {}: {e}", index.display()))?;
    let sig_bytes =
        std::fs::read(&sig).map_err(|e| format!("failed to read {}: {e}", sig.display()))?;
    let parsed = registry_index::load_verified_index(&trust, &bytes, &sig_bytes)?;
    let rows: usize = parsed.apps.iter().map(|a| a.compat.len()).sum();
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "verified": true,
                "index": index,
                "channel": parsed.channel,
                "generated_at": parsed.generated_at,
                "apps": parsed.apps.len(),
                "rows": rows,
            }))
            .unwrap_or_default()
        );
    } else {
        println!(
            "verified {}: channel {} · {} apps · {} rows · generated {}",
            index.display(),
            parsed.channel,
            parsed.apps.len(),
            rows,
            parsed.generated_at
        );
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
