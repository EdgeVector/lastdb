//! Explicit owner repair of cloud copies after main-namespace atom reclamation.
//! The private plan is erase intent, not a permanent atom-identity registry.
//! Cloud remains Off on every outcome. A prepared manifest journal reconciles
//! an uncertain CAS before the local predecessor mirror changes.

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fold_db::hex::sha256_hex;
use fold_db::storage::laststore::{
    cloud_db_hash_for_store_uuid, manifest_sha256_hex, BackupManifest,
};
use fold_db::sync::auth::{AuthClient, SyncAuth};
use fold_db::sync::engine::backup_atom_rewrite::{
    prepare_cloud_backup_atom_rewrite, publish_cloud_backup_atom_rewrite,
};
use fold_db::sync::s3::S3Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    version: u32,
    source_store_uuid: String,
    expected_manifest_sha256: String,
    chunk_shas: Vec<String>,
    atom_ids: Vec<String>,
    expires_at_unix_secs: u64,
    user_authorized: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u32,
    plan_sha256: String,
    manifest: BackupManifest,
}

fn now() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_secs())
        .map_err(|_| "clock before epoch".into())
}

fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn validate_plan(plan: &Plan, time: u64) -> Result<(), String> {
    if plan.version != 1
        || !plan.user_authorized
        || plan.source_store_uuid.is_empty()
        || !valid_sha(&plan.expected_manifest_sha256)
        || plan.expires_at_unix_secs <= time
        || plan.expires_at_unix_secs > time.saturating_add(7200)
    {
        return Err("invalid, unauthorized, or expired rewrite plan".into());
    }
    for ids in [&plan.chunk_shas, &plan.atom_ids] {
        if ids.is_empty()
            || ids.len() > 64
            || ids.iter().any(|id| !valid_sha(id))
            || ids.iter().collect::<BTreeSet<_>>().len() != ids.len()
        {
            return Err("invalid or repeated rewrite selection".into());
        }
    }
    Ok(())
}

fn private_read(path: &Path, cap: u64) -> Result<Vec<u8>, String> {
    let meta = fs::symlink_metadata(path).map_err(|_| "private input unavailable")?;
    if !meta.is_file() || meta.mode() & 0o077 != 0 || meta.len() > cap {
        return Err("private input must be a bounded owner-only regular file".into());
    }
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|_| "private input open failed")?
        .take(cap + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "private input read failed")?;
    if bytes.len() as u64 > cap {
        return Err("private input exceeds budget".into());
    }
    Ok(bytes)
}

fn atomic_private_write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension(format!("rewrite-{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(|_| "private atomic file create failed")?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| "private atomic file sync failed")?;
    fs::rename(&tmp, path).map_err(|_| "private atomic file rename failed")?;
    fs::File::open(path.parent().ok_or("private file parent absent")?)
        .and_then(|f| f.sync_all())
        .map_err(|_| "private parent sync failed".to_string())
}

fn owner_get(home: &Path, route: &str) -> Result<(u16, serde_json::Value), String> {
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let request = format!("GET {route} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    let response =
        super::request_with_timeout(&socket, request.as_bytes(), Duration::from_secs(120))
            .map_err(|_| "owner socket request failed")?;
    parse_owner_response(route, &response)
}

fn parse_owner_response(route: &str, response: &str) -> Result<(u16, serde_json::Value), String> {
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or("invalid owner response")?;
    let code = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or("invalid owner status")?;
    if code == 404 {
        if let Some(atom) = route.strip_prefix("/api/atom/") {
            if valid_sha(atom) && body == format!("Atom '{atom}' not found") {
                return Ok((code, serde_json::Value::Null));
            }
        }
        return Err("owner response does not prove the selected atom is absent".into());
    }
    let value = serde_json::from_str(body).map_err(|_| "invalid owner response JSON")?;
    Ok((code, value))
}

fn check_owner(home: &Path, plan: &Plan) -> Result<(), String> {
    if home.join("cloud_sync.json").exists() || !home.join("cloud_sync.json.paused").is_file() {
        return Err("rewrite requires durable cloud Off".into());
    }
    let (code, status) = owner_get(home, "/api/status")?;
    if code != 200
        || !status
            .pointer("/status/sync/cloud_sync_disabled_at")
            .is_some_and(serde_json::Value::is_number)
    {
        return Err("live owner does not confirm cloud Off".into());
    }
    let high_water: serde_json::Value = serde_json::from_slice(
        &fs::read(home.join("laststore_high_water.json"))
            .map_err(|_| "owner identity unavailable")?,
    )
    .map_err(|_| "invalid owner identity")?;
    if high_water.get("store_uuid").and_then(|v| v.as_str())
        != Some(plan.source_store_uuid.as_str())
    {
        return Err("rewrite source differs from owner identity".into());
    }
    for atom in &plan.atom_ids {
        if owner_get(home, &format!("/api/atom/{atom}"))?.0 != 404 {
            return Err("selected atom is present or its absence is unproved".into());
        }
    }
    Ok(())
}

pub(super) async fn run(
    home: &Path,
    plan_path: &Path,
    execute: bool,
    json: bool,
) -> Result<(), String> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(home.join("laststore_atom_rewrite.lock"))
        .map_err(|_| "rewrite lock unavailable")?;
    lock.try_lock()
        .map_err(|_| "another owner rewrite holds the lock")?;
    let plan_bytes = private_read(plan_path, 64 * 1024)?;
    let plan: Plan =
        serde_json::from_slice(&plan_bytes).map_err(|_| "invalid rewrite plan JSON")?;
    validate_plan(&plan, now()?)?;
    check_owner(home, &plan)?;
    let config: serde_json::Value = serde_json::from_slice(&private_read(
        &home.join("cloud_sync.json.paused"),
        64 * 1024,
    )?)
    .map_err(|_| "invalid paused cloud config")?;
    let url = config
        .get("api_url")
        .and_then(|v| v.as_str())
        .ok_or("paused cloud URL absent")?
        .to_owned();
    let key = config
        .get("api_key")
        .and_then(|v| v.as_str())
        .ok_or("paused cloud key absent")?
        .to_owned();
    let http = Arc::new(fold_db::sync::build_shared_http_client());
    let auth = AuthClient::new(Arc::clone(&http), url, SyncAuth::ApiKey(key))
        .with_db_hash(Some(cloud_db_hash_for_store_uuid(&plan.source_store_uuid)))
        .without_db_auto_claim();
    let s3 = S3Client::new(http);
    let plan_sha = sha256_hex(&plan_bytes);
    let journal_path = home.join(format!("laststore_atom_rewrite_{plan_sha}.json"));
    let mut reconciled = false;
    let (manifest, removed) = if journal_path.exists() {
        let journal: Journal =
            serde_json::from_slice(&private_read(&journal_path, 16 * 1024 * 1024)?)
                .map_err(|_| "invalid rewrite journal")?;
        if journal.version != 1
            || journal.plan_sha256 != plan_sha
            || journal.manifest.store_uuid != plan.source_store_uuid
            || journal.manifest.previous_manifest_sha256.as_deref()
                != Some(&plan.expected_manifest_sha256)
        {
            return Err("rewrite journal does not match the owner plan".into());
        }
        let latest = auth
            .backup_latest_get()
            .await
            .map_err(|_| "cloud tip read failed; leave cloud Off")?;
        let committed_sha =
            manifest_sha256_hex(&journal.manifest).map_err(|_| "invalid journal manifest")?;
        if latest.latest.manifest_sha256 == plan.expected_manifest_sha256 {
            // No CAS landed. Rebuild the exact journaled successor with the
            // original timestamp, verify its digest, then retry normal CAS.
            let prepared = prepare_cloud_backup_atom_rewrite(
                &auth,
                &s3,
                &plan.expected_manifest_sha256,
                &plan.chunk_shas.iter().cloned().collect(),
                &plan.atom_ids.iter().cloned().collect(),
                journal.manifest.created_at_unix_secs,
            )
            .await
            .map_err(|_| "journal revalidation failed; leave cloud Off")?;
            if manifest_sha256_hex(prepared.manifest()).map_err(|_| "invalid prepared manifest")?
                != committed_sha
            {
                return Err("journal differs from rederived rewrite; leave cloud Off".into());
            }
            if execute {
                validate_plan(&plan, now()?)?;
                check_owner(home, &plan)?;
                publish_cloud_backup_atom_rewrite(&auth, &s3, &prepared)
                    .await
                    .map_err(|_| "journal CAS retry failed or uncertain; leave cloud Off")?;
            }
        } else if latest.latest.manifest_sha256 != committed_sha
            || latest.latest.store_uuid != journal.manifest.store_uuid
            || latest.latest.epoch != journal.manifest.epoch
            || latest.latest.counter != journal.manifest.counter
        {
            return Err("prepared rewrite lacks its exact CAS receipt; leave cloud Off and inspect the journal".into());
        }
        reconciled = true;
        (journal.manifest, None)
    } else {
        let prepared = prepare_cloud_backup_atom_rewrite(
            &auth,
            &s3,
            &plan.expected_manifest_sha256,
            &plan.chunk_shas.iter().cloned().collect(),
            &plan.atom_ids.iter().cloned().collect(),
            now()?,
        )
        .await
        .map_err(|_| "cloud rewrite preparation failed; no CAS attempted")?;
        let removed = Some(prepared.removed_records());
        if execute {
            validate_plan(&plan, now()?)?;
            check_owner(home, &plan)?;
            let journal = Journal {
                version: 1,
                plan_sha256: plan_sha,
                manifest: prepared.manifest().clone(),
            };
            atomic_private_write(
                &journal_path,
                &serde_json::to_vec(&journal).map_err(|_| "journal encode failed")?,
            )?;
            let committed = publish_cloud_backup_atom_rewrite(&auth, &s3, &prepared).await
                .map_err(|_| "cloud rewrite publication failed or uncertain; leave cloud Off and reconcile the journal")?;
            (committed, removed)
        } else {
            (prepared.manifest().clone(), removed)
        }
    };
    if execute {
        check_owner(home, &plan)?;
        atomic_private_write(
            &lastdb_node::host::backup_manifest_cache_path(home),
            &serde_json::to_vec(&manifest).map_err(|_| "manifest mirror encode failed")?,
        )?;
    }
    let result = serde_json::json!({"ok": true, "executed": execute, "reconciled": reconciled,
        "cloud_remains_off": true, "counter": manifest.counter,
        "manifest_sha256": manifest_sha256_hex(&manifest).map_err(|_| "manifest digest failed")?,
        "retired_chunks": plan.chunk_shas.len(), "removed_records": removed});
    if json {
        println!("{result}");
    } else {
        println!("Cloud atom rewrite verified. Executed: {execute}. Cloud remains Off.");
    }
    Ok(())
}
