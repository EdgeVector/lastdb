//! Cloud intent, quarantine, manifest cut and staging heal commands. Moved verbatim from `cloud_cmds.rs`.

use super::*;

/// Live clear of one `replay_blocker` via owner socket.
pub(crate) fn cloud_quarantine_replay(
    home: &Path,
    target: &str,
    seq: u64,
    json_only: bool,
) -> Result<(), String> {
    let socket = home.join("data").join("folddb.sock");
    if !socket.exists() {
        return Err(format!(
            "daemon socket missing at {} — start lastdbd first",
            socket.display()
        ));
    }
    let response = post_json(
        &socket,
        "/api/sync/quarantine-replay-blocker",
        &serde_json::json!({ "target": target, "seq": seq }),
    )?;
    let value = parse_json_response(&response, "quarantine-replay")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
        );
        return Ok(());
    }
    let data = value.get("data").unwrap_or(&value);
    println!("Cleared cloud replay pin:");
    if let Some(t) = data.get("target") {
        println!("  target:              {t}");
    }
    if let Some(s) = data.get("seq") {
        println!("  seq:                 {s}");
    }
    if let Some(m) = data.get("mode") {
        println!("  mode:                {m}");
    }
    if let Some(d) = data.get("deleted_log_objects") {
        println!("  deleted_log_objects: {d}");
    }
    if let Some(n) = data.get("note").and_then(|x| x.as_str()) {
        println!("  note: {n}");
    }
    Ok(())
}

/// Live `POST /api/sync/cloud-on` or `/cloud-off` on the owner socket.
pub(crate) fn cloud_set_intent(home: &Path, on: bool, json_only: bool) -> Result<(), String> {
    let socket = home.join("data").join("folddb.sock");
    let path = if on {
        "/api/sync/cloud-on"
    } else {
        "/api/sync/cloud-off"
    };
    if !socket.exists() {
        // Daemon down: a durable pause is safe. Cloud Sync cannot resume here.
        if on {
            if lastdb_node::cloud::cloud_sync_file_state(home) == "unset" {
                return Err("cloud sync is not configured".into());
            }
            return Err("Cloud Sync remains Off: start the daemon and check its sync status before you use `lastdb cloud on`".into());
        }
        let renamed = lastdb_node::cloud::pause_cloud_sync_file(home)?;
        if json_only {
            println!(
                "{}",
                serde_json::json!({
                    "ok": true,
                    "intent": "off",
                    "file_state": lastdb_node::cloud::cloud_sync_file_state(home),
                    "file_renamed": renamed,
                    "daemon": "down",
                    "note": "next lastdbd boot will not start cloud sync",
                })
            );
        } else {
            println!(
                "Cloud Sync OFF (file only; daemon socket missing at {})",
                socket.display()
            );
            println!(
                "  file_state: {} (renamed={renamed})",
                lastdb_node::cloud::cloud_sync_file_state(home)
            );
            println!("  next lastdbd boot will not start cloud sync");
        }
        return Ok(());
    }
    let response = post_json(&socket, path, &serde_json::json!({}))?;
    let value = parse_json_response(&response, if on { "cloud-on" } else { "cloud-off" })?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
        );
        return Ok(());
    }
    let data = value.get("data").unwrap_or(&value);
    if on {
        println!("Cloud Sync ON:");
    } else {
        println!("Cloud Sync OFF (intentional pause):");
    }
    if let Some(s) = data.get("file_state").and_then(|v| v.as_str()) {
        println!("  file_state:              {s}");
    }
    if let Some(v) = data
        .get("file_renamed")
        .or_else(|| data.get("file_restored"))
    {
        println!("  file_changed:            {v}");
    }
    if let Some(n) = data.get("engine_note").and_then(|v| v.as_str()) {
        println!("  engine: {n}");
    }
    if let Some(v) = data.get("recording_local_changes") {
        println!("  recording_local_changes: {v}");
    }
    if let Some(v) = data.get("sync_off_grace_expired") {
        println!("  sync_off_grace_expired:  {v}");
    }
    if let Some(v) = data.get("reenable_strategy") {
        if !v.is_null() {
            println!("  reenable_strategy:       {v}");
        }
    }
    if let Some(n) = data.get("note").and_then(|v| v.as_str()) {
        println!("  note: {n}");
    }
    Ok(())
}

pub(super) fn cloud_prepare_resume_primary(home: &Path, json_only: bool) -> Result<(), String> {
    let renamed = lastdb_node::cloud::prepare_primary_resume_file(home)?;
    let report = serde_json::json!({
        "ok": true,
        "state": "pending_restart",
        "file_state": lastdb_node::cloud::cloud_sync_file_state(home),
        "resume_required": lastdb_node::cloud::cloud_resume_required_path(home).exists(),
        "file_renamed": renamed,
    });
    if json_only {
        println!("{report}");
    } else {
        println!("Primary resume is ready for a supervised daemon restart.");
        println!("  Cloud Sync remains Off until the resume job passes.");
    }
    Ok(())
}

pub(crate) fn cloud_cut_manifest(
    home: &Path,
    previous: Option<PathBuf>,
    out: Option<&Path>,
    json_only: bool,
) -> Result<(), String> {
    let data_path = home.join("data");
    if !data_path.is_dir() {
        return Err(format!(
            "missing LastStore data dir {}",
            data_path.display()
        ));
    }

    let seed_path = home.join(lastdb_node::host::IDENTITY_KEY_FILE);
    let seed_bytes =
        std::fs::read(&seed_path).map_err(|e| format!("read {}: {e}", seed_path.display()))?;
    if seed_bytes.len() != 32 {
        return Err(format!(
            "{} must be 32 bytes, got {}",
            seed_path.display(),
            seed_bytes.len()
        ));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&seed_bytes);
    let e2e = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
        .map_err(|e| format!("E2E derive: {e}"))?;

    let previous_manifest = match previous {
        Some(path) => {
            let bytes =
                std::fs::read(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
            let manifest: fold_db::storage::laststore::BackupManifest =
                serde_json::from_slice(&bytes)
                    .map_err(|e| format!("parse previous manifest {}: {e}", path.display()))?;
            Some(manifest)
        }
        None => None,
    };

    let store = fold_db::storage::LastStoreNamespacedStore::open_with_data_key_and_high_water(
        &data_path,
        e2e.encryption_key(),
        home.join("laststore_high_water.json"),
    )
    .map_err(|e| format!("open LastStore: {e}"))?;

    let manifest = store
        .cut_backup_manifest(previous_manifest.as_ref())
        .map_err(|e| format!("cut backup manifest: {e}"))?;
    fold_db::storage::laststore::validate_manifest_chain(previous_manifest.as_ref(), &manifest)
        .map_err(|e| format!("validate backup manifest chain: {e}"))?;

    let encoded = serde_json::to_string_pretty(&manifest)
        .map_err(|e| format!("encode backup manifest: {e}"))?;
    let wrote_out = out.is_some();
    if let Some(path) = out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create {}: {e}", parent.display()))?;
        }
        std::fs::write(path, encoded.as_bytes())
            .map_err(|e| format!("write {}: {e}", path.display()))?;
        if !json_only {
            println!("backup manifest written: {}", path.display());
        }
    }
    if json_only || !wrote_out {
        println!("{encoded}");
    } else {
        println!(
            "counter={} cut_csn={} atom_chunks={} mutable_chunks={}",
            manifest.counter,
            manifest.cut_csn,
            manifest.atom_chunks.len(),
            manifest.mutable_chunks.len()
        );
    }
    Ok(())
}

/// Live heal: POST /api/sync/heal-staging on the owner socket (daemon keeps running).
pub(crate) fn cloud_heal_staging(home: &Path, json_only: bool) -> Result<(), String> {
    let socket = home.join("data").join("folddb.sock");
    if !socket.exists() {
        return Err(format!(
            "daemon socket missing at {} — start lastdbd first (do not stop Mini for this heal)",
            socket.display()
        ));
    }
    let response = post_json_admin(&socket, "/api/sync/heal-staging", &serde_json::json!({}))?;
    let value = parse_json_response(&response, "heal-staging")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
        );
        return Ok(());
    }
    let data = value.get("data").unwrap_or(&value);
    println!("Live cloud staging heal (Mini stayed up for local R/W):");
    if let Some(seq) = data.get("snapshot_seq") {
        println!("  snapshot_seq:     {seq}");
    }
    if let Some(v) = data.get("staging_before") {
        println!("  staging_before:   {v}");
    }
    if let Some(v) = data.get("staging_cleared") {
        println!("  staging_cleared:  {v}");
    }
    if let Some(v) = data.get("staging_after") {
        println!("  staging_after:    {v}");
    }
    if let Some(n) = data.get("note").and_then(|x| x.as_str()) {
        println!("  note: {n}");
    }
    Ok(())
}
