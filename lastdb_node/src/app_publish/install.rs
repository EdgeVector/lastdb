//! Install, upgrade, and run installed apps.

use super::*;

pub async fn list_apps(schema_service_url: &str) -> Result<Vec<AppRecord>, String> {
    let client = SchemaServiceClient::new(schema_service_url);
    client
        .fetch_apps()
        .await
        .map_err(|e| format!("registry app list fetch failed: {e}"))
}

pub async fn app_info(schema_service_url: &str, app_id: &str) -> Result<Value, String> {
    let client = SchemaServiceClient::new(schema_service_url);
    let lookup = client
        .fetch_app(app_id)
        .await
        .map_err(|e| format!("app lookup failed: {e}"))?
        .ok_or_else(|| format!("app '{app_id}' is not in the registry"))?;
    // `AppLookup` is deserialize-only; re-shape it for display.
    Ok(json!({
        "app_id": lookup.app_id,
        "owner_dev_pubkey": lookup.owner_dev_pubkey,
        "metadata": lookup.metadata,
        "version": lookup.version,
        "registered_at": lookup.registered_at,
        "tier": lookup.tier,
        "code_signature": lookup.code_signature,
        "source": lookup.source,
        "artifact": lookup.artifact,
        "uses": lookup.uses,
        "revoked": lookup.revoked,
    }))
}

// ─── Install surface ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct InstallOutcome {
    pub app_id: String,
    pub version: String,
    pub install_dir: String,
    pub checkout_path: String,
    pub source: String,
    pub tier: String,
    pub owner_dev_pubkey: String,
    pub code_signature_declared: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpgradeOutcome {
    pub app_id: String,
    pub installed_version: String,
    pub registry_version: String,
    pub upgraded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub install: Option<InstallOutcome>,
}

#[derive(Debug, Deserialize)]
pub(super) struct InstallReceipt {
    #[serde(default = "schema_service_core::snapshot::default_app_version")]
    version: String,
}

// lint:fn-size-ok moved verbatim from its original module; no logic change
pub async fn install_app(
    schema_service_url: &str,
    app_id: &str,
    install_dir: &Path,
    allow_sandbox: bool,
    force: bool,
) -> Result<InstallOutcome, String> {
    let client = SchemaServiceClient::new(schema_service_url);
    let lookup = client
        .fetch_app(app_id)
        .await
        .map_err(|e| format!("app lookup failed: {e}"))?
        .ok_or_else(|| format!("app '{app_id}' is not in the registry"))?;
    if lookup.revoked {
        return Err(format!(
            "install rejected: app '{app_id}' publisher is revoked"
        ));
    }
    let tier = match lookup.tier {
        AppTier::Live => "live",
        AppTier::Sandbox => "sandbox",
    };
    if lookup.tier != AppTier::Live && !allow_sandbox {
        return Err(format!(
            "install rejected: app '{app_id}' is {tier}; pass --allow-sandbox to install sandbox records"
        ));
    }
    if lookup.owner_dev_pubkey.trim().is_empty() {
        return Err(format!(
            "install rejected: app '{app_id}' registry record has no publisher key"
        ));
    }
    let source = lookup.source.clone().ok_or_else(|| {
        if lookup.artifact.is_some() {
            format!(
                "install rejected: app '{app_id}' only has an artifact pointer; signed artifact download is not implemented yet"
            )
        } else {
            format!("install rejected: app '{app_id}' has no source or artifact pointer")
        }
    })?;
    let checkout_path = install_dir.join("source");
    if install_dir.exists() {
        if !force {
            return Err(format!(
                "install directory already exists: {} (pass --force to replace it)",
                install_dir.display()
            ));
        }
        std::fs::remove_dir_all(install_dir).map_err(|e| {
            format!(
                "failed to remove existing install directory {}: {e}",
                install_dir.display()
            )
        })?;
    }
    if let Some(parent) = install_dir.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            format!(
                "failed to create install parent directory {}: {e}",
                parent.display()
            )
        })?;
    }
    std::fs::create_dir_all(install_dir).map_err(|e| {
        format!(
            "failed to create install directory {}: {e}",
            install_dir.display()
        )
    })?;
    let clone_status = ProcessCommand::new("git")
        .arg("clone")
        .arg("--depth=1")
        .arg(&source)
        .arg(&checkout_path)
        .status()
        .map_err(|e| format!("failed to run git clone: {e}"))?;
    if !clone_status.success() {
        let _ = std::fs::remove_dir_all(install_dir);
        return Err(format!(
            "git clone failed for app '{app_id}' from {source} into {}",
            checkout_path.display()
        ));
    }

    let code_signature_declared = lookup.code_signature.is_some();
    let app_id = lookup.app_id.clone();
    let owner_dev_pubkey = lookup.owner_dev_pubkey.clone();
    let version = lookup.version.clone();
    let install_record = json!({
        "app_id": app_id,
        "version": version,
        "installed_at": chrono::Utc::now().to_rfc3339(),
        "schema_service_url": schema_service_url,
        "source": source,
        "tier": tier,
        "owner_dev_pubkey": owner_dev_pubkey,
        "metadata": lookup.metadata,
        "code_signature": lookup.code_signature,
        "artifact": lookup.artifact,
        "uses": lookup.uses,
    });
    let record_path = install_dir.join("lastdb-app-install.json");
    let record_bytes = serde_json::to_vec_pretty(&install_record)
        .map_err(|e| format!("failed to encode install record: {e}"))?;
    std::fs::write(&record_path, record_bytes)
        .map_err(|e| format!("failed to write {}: {e}", record_path.display()))?;

    Ok(InstallOutcome {
        app_id,
        version,
        install_dir: install_dir.display().to_string(),
        checkout_path: checkout_path.display().to_string(),
        source,
        tier: tier.to_string(),
        owner_dev_pubkey,
        code_signature_declared,
    })
}

pub async fn upgrade_app(
    schema_service_url: &str,
    app_id: &str,
    install_dir: &Path,
    allow_sandbox: bool,
) -> Result<UpgradeOutcome, String> {
    let receipt_path = install_dir.join("lastdb-app-install.json");
    if !receipt_path.exists() {
        let install = install_app(
            schema_service_url,
            app_id,
            install_dir,
            allow_sandbox,
            false,
        )
        .await?;
        return Ok(UpgradeOutcome {
            app_id: app_id.to_string(),
            installed_version: "0.0.0".to_string(),
            registry_version: install.version.clone(),
            upgraded: true,
            install: Some(install),
        });
    }
    let receipt_raw = std::fs::read_to_string(&receipt_path).map_err(|e| {
        format!(
            "failed to read install receipt {}: {e}",
            receipt_path.display()
        )
    })?;
    let receipt: InstallReceipt = serde_json::from_str(&receipt_raw)
        .map_err(|e| format!("invalid install receipt {}: {e}", receipt_path.display()))?;
    validate_app_version(&receipt.version).map_err(|e| {
        format!(
            "invalid installed version in {}: {e}",
            receipt_path.display()
        )
    })?;

    let client = SchemaServiceClient::new(schema_service_url);
    let lookup = client
        .fetch_app(app_id)
        .await
        .map_err(|e| format!("app lookup failed: {e}"))?
        .ok_or_else(|| format!("app '{app_id}' is not in the registry"))?;
    validate_app_version(&lookup.version)
        .map_err(|e| format!("registry record for app '{app_id}' has invalid version: {e}"))?;
    let should_upgrade = app_version_is_greater(&lookup.version, &receipt.version)?;
    if !should_upgrade {
        return Ok(UpgradeOutcome {
            app_id: app_id.to_string(),
            installed_version: receipt.version,
            registry_version: lookup.version,
            upgraded: false,
            install: None,
        });
    }

    let install = install_app(schema_service_url, app_id, install_dir, allow_sandbox, true).await?;
    Ok(UpgradeOutcome {
        app_id: app_id.to_string(),
        installed_version: receipt.version,
        registry_version: install.version.clone(),
        upgraded: true,
        install: Some(install),
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct RunOutcome {
    pub app_id: String,
    pub install_dir: String,
    pub checkout_path: String,
    pub runtime: String,
    pub entrypoint: String,
    pub exit_code: i32,
}

pub fn run_installed_app(
    home: &Path,
    app_id: &str,
    install_dir: &Path,
    extra_args: &[String],
) -> Result<RunOutcome, String> {
    let record_path = install_dir.join("lastdb-app-install.json");
    let record: Value = serde_json::from_slice(&std::fs::read(&record_path).map_err(|e| {
        format!(
            "app '{app_id}' is not installed at {} (missing {}): {e}",
            install_dir.display(),
            record_path.display()
        )
    })?)
    .map_err(|e| format!("invalid install receipt {}: {e}", record_path.display()))?;
    let installed_id = record
        .get("app_id")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("install receipt {} missing app_id", record_path.display()))?;
    if installed_id != app_id {
        return Err(format!(
            "install receipt mismatch: requested app '{app_id}', receipt is for '{installed_id}'"
        ));
    }

    let checkout_path = install_dir.join("source");
    let manifest_path = checkout_path.join("lastdb-app.json");
    let manifest = load_manifest(&manifest_path)?;
    if manifest.app_id != app_id {
        return Err(format!(
            "source manifest mismatch: requested app '{app_id}', manifest is for '{}'",
            manifest.app_id
        ));
    }
    let run = manifest.run.as_ref().ok_or_else(|| {
        format!("app '{app_id}' has no runnable entrypoint (manifest missing `run` block)")
    })?;
    let entrypoint = safe_source_entrypoint(&checkout_path, &run.entrypoint)?;
    let (program, mut args) = runner_command(run, &entrypoint, app_id)?;
    args.extend(run.args.iter().map(OsString::from));
    args.extend(extra_args.iter().map(OsString::from));

    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let status = ProcessCommand::new(&program)
        .args(&args)
        .current_dir(&checkout_path)
        .env("LASTDB_HOME", home)
        .env("FOLDDB_HOME", home)
        .env("LASTDB_DATA_DIR", home.join("data"))
        .env("LASTDB_SOCKET", &socket)
        .env("LASTDB_APP_ID", app_id)
        .env("LASTDB_APP_INSTALL_DIR", install_dir)
        .env("LASTDB_APP_SOURCE_DIR", &checkout_path)
        .status()
        .map_err(|e| {
            format!(
                "failed to run app '{app_id}' entrypoint {} with runtime '{}': {e}",
                run.entrypoint, run.runtime
            )
        })?;
    if !status.success() {
        return Err(format!(
            "app '{app_id}' exited with status {}",
            status
                .code()
                .map_or_else(|| "terminated by signal".to_string(), |c| c.to_string())
        ));
    }
    Ok(RunOutcome {
        app_id: app_id.to_string(),
        install_dir: install_dir.display().to_string(),
        checkout_path: checkout_path.display().to_string(),
        runtime: run.runtime.clone(),
        entrypoint: run.entrypoint.clone(),
        exit_code: status.code().unwrap_or(0),
    })
}

pub(super) fn safe_source_entrypoint(
    source_dir: &Path,
    entrypoint: &str,
) -> Result<PathBuf, String> {
    if entrypoint.trim().is_empty() {
        return Err("run entrypoint must be non-empty".into());
    }
    let rel = Path::new(entrypoint);
    if rel.is_absolute() {
        return Err(format!(
            "run entrypoint must be relative to the installed source checkout: {entrypoint}"
        ));
    }
    if rel.components().any(|c| {
        matches!(
            c,
            std::path::Component::ParentDir | std::path::Component::Prefix(_)
        )
    }) {
        return Err(format!(
            "run entrypoint must stay inside the installed source checkout: {entrypoint}"
        ));
    }
    let path = source_dir.join(rel);
    if !path.is_file() {
        return Err(format!("run entrypoint not found: {}", path.display()));
    }
    Ok(path)
}

pub(super) fn runner_command(
    run: &AppRunConfig,
    entrypoint: &Path,
    app_id: &str,
) -> Result<(OsString, Vec<OsString>), String> {
    match run.runtime.trim() {
        "command" => Ok((entrypoint.as_os_str().to_os_string(), Vec::new())),
        "sh" => Ok((OsString::from("sh"), vec![entrypoint.as_os_str().to_os_string()])),
        "python3" => Ok((
            OsString::from("python3"),
            vec![entrypoint.as_os_str().to_os_string()],
        )),
        "node" => Ok((
            OsString::from("node"),
            vec![entrypoint.as_os_str().to_os_string()],
        )),
        "bun" => Ok((OsString::from("bun"), vec![entrypoint.as_os_str().to_os_string()])),
        other => Err(format!(
            "unsupported runtime '{other}' for app '{app_id}' (supported: command, sh, python3, node, bun)"
        )),
    }
}

// ─── Developer signing key (dev-init) ─────────────────────────────────────
