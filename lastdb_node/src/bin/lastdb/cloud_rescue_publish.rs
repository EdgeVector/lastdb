//! One-shot S0 publisher for an inert stopped home. Never changes backup/latest.

use self::writer_inventory_proof::{
    read_prior_backup_manifest, require_first_backup_writer_inventory, save_writer_inventory_proof,
};
use base64::Engine as _;
use fold_db::hex::sha256_hex;
use fold_db::storage::laststore::{
    cloud_db_hash_for_store_uuid, manifest_sha256_hex, validate_manifest_chain, BackupChunkRef,
    BackupChunkUploadCandidate, BackupDeletionReceipt, BackupManifest,
};
use fold_db::sync::auth::ops::{RescueS0CommitOutcome, RescueS0Identity, RescueS0Pointer};
use fold_db::sync::engine::{
    prepare_offline_s0_restore_marker, require_offline_s0_restore_marker, RecoveryDescriptorV1,
    PIN_LOG_NAMESPACE,
};
use fold_db::sync::error::SyncError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[path = "cloud_rescue_publish/files.rs"]
mod files;
#[path = "cloud_rescue_publish/inventory.rs"]
mod inventory;
#[path = "cloud_rescue_publish/plan.rs"]
mod plan;
#[path = "cloud_rescue_publish/upload.rs"]
mod upload;
#[path = "cloud_rescue_publish/validate.rs"]
mod validate;
use files::*;
use inventory::*;
use plan::*;
use upload::*;
use validate::*;

const PLAN_FILE: &str = ".rescue_s0_plan_v1.json";
const COMMITTED_FILE: &str = ".rescue_s0_committed_v1.json";
const PAGE_FILE_PREFIX: &str = ".rescue_s0_page_v1_";
const WRITER_PROOF_FILE: &str = ".rescue_s0_writer_proof_v1.json";
const MAX_PLAN_BYTES: u64 = 20 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_CHUNK_BYTES: u64 = 128 * 1024 * 1024;
const MAX_CHUNKS: usize = 100_000;
const MAX_WAIT_SECS: u64 = 3_600;
const MAX_PEER_LOG_OBJECTS: usize = 250_000;
const UPLOAD_CONCURRENCY: usize = 16;
const MAX_CHUNK_ATTEMPTS: usize = 4;
const CHUNK_RETRY_BASE_DELAY_MS: u64 = 500;
// Cloud confirmation hashes the stored chunk inside a 30-second Lambda call.
// Keep large confirmations clear of this publisher's other cloud requests.
const ISOLATED_CONFIRM_MIN_BYTES: u64 = 50 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RescuePlan {
    version: u32,
    source: lastdb_node::cloud::BackupSourceCopyMarker,
    manifest: BackupManifest,
    manifest_sha256: String,
    db_hash: String,
    descriptor_name: String,
    descriptor_sha256: String,
    descriptor_base64: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RescuePageMarker {
    version: u32,
    manifest_sha256: String,
    prefix: String,
    chunks_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriterInventoryProof {
    version: u32,
    source_scope: String,
    rescue_manifest_sha256: String,
    db_hash: String,
    source: lastdb_node::cloud::BackupSourceCopyMarker,
    #[serde(default)]
    prior_manifest_present: Option<bool>,
    prior_manifest_sha256: Option<String>,
    prior_manifest_counter: Option<u64>,
    visible_log_object_count: usize,
    unparsed_log_objects: u64,
    inspected_at_unix_secs: u64,
    inventory: serde_json::Value,
}

type LoadedRescuePlan = (
    RescuePlan,
    BTreeMap<String, BackupChunkUploadCandidate>,
    [u8; 32],
);

impl RescuePlan {
    fn identity(&self) -> RescueS0Identity {
        RescueS0Identity {
            db_hash: self.db_hash.clone(),
            manifest_sha256: self.manifest_sha256.clone(),
            descriptor_name: self.descriptor_name.clone(),
            descriptor_sha256: self.descriptor_sha256.clone(),
        }
    }

    fn manifest_bytes(&self) -> Result<Vec<u8>, String> {
        let bytes = serde_json::to_vec(&self.manifest)
            .map_err(|error| format!("encode S0 rescue manifest: {error}"))?;
        if bytes.is_empty() || bytes.len() > MAX_MANIFEST_BYTES {
            return Err("S0 rescue manifest exceeds the service size limit".into());
        }
        Ok(bytes)
    }

    fn descriptor_bytes(&self, e2e_key: &[u8; 32]) -> Result<Vec<u8>, String> {
        let ciphertext = base64::engine::general_purpose::STANDARD
            .decode(&self.descriptor_base64)
            .map_err(|_| "invalid saved S0 recovery descriptor bytes")?;
        if ciphertext.is_empty() || ciphertext.len() > 65_536 {
            return Err("saved S0 recovery descriptor exceeds its size limit".into());
        }
        if sha256_hex(&ciphertext) != self.descriptor_sha256 {
            return Err("saved S0 recovery descriptor hash mismatch".into());
        }
        let descriptor = RecoveryDescriptorV1::open(&self.descriptor_name, &ciphertext, e2e_key)?;
        if descriptor.store_uuid != self.manifest.store_uuid
            || descriptor.db_hash != self.db_hash
            || descriptor.manifest_sha256 != self.manifest_sha256
            || descriptor.counter != self.manifest.counter
            || descriptor.epoch != self.manifest.epoch
        {
            return Err("saved S0 recovery descriptor does not match the manifest".into());
        }
        Ok(ciphertext)
    }
}

pub(super) async fn run(
    home: &Path,
    execute: bool,
    wait: bool,
    inspect: bool,
    json: bool,
) -> Result<(), String> {
    if inspect && execute {
        return Err("writer inspection cannot publish an S0 rescue".into());
    }
    if inspect
        && std::fs::symlink_metadata(home.join(lastdb_node::host::CLOUD_SYNC_CONFIG_FILE)).is_ok()
    {
        return Err("writer inspection requires Cloud Sync Off".into());
    }
    let paused = home
        .join(lastdb_node::host::CLOUD_SYNC_CONFIG_FILE)
        .with_extension("json.paused");
    if inspect {
        read_small_regular(&paused, 65_536)?
            .ok_or("writer inspection requires the paused cloud configuration")?;
        let db_hash = fold_db::storage::laststore::read_cloud_db_hash(&home.join("data"))
            .ok_or("writer inspection requires the local cloud database identity")?;
        let (url, api_key) = super::load_cloud_creds_from_path(&paused, None, None)?;
        let http = std::sync::Arc::new(fold_db::sync::build_shared_http_client());
        let s3 = fold_db::sync::s3::S3Client::new(std::sync::Arc::clone(&http));
        let auth = fold_db::sync::auth::AuthClient::new(
            http,
            url,
            fold_db::sync::auth::SyncAuth::ApiKey(api_key),
        )
        .without_db_auto_claim();
        let report = collect_writer_inventory(home, &db_hash, &auth, &s3).await?;
        print_writer_inventory(&report, json);
        return Ok(());
    }
    let (plan, candidates, key) = load_or_create_plan(home, execute).await?;
    let pages = page_groups(&candidates)?;
    if !execute {
        print_report(&plan, candidates.len(), false, json);
        return Ok(());
    }
    let (url, api_key) = super::load_cloud_creds_from_path(&paused, None, None)?;
    let http = std::sync::Arc::new(fold_db::sync::build_shared_http_client());
    let s3 = fold_db::sync::s3::S3Client::new(std::sync::Arc::clone(&http));
    let auth = fold_db::sync::auth::AuthClient::new(
        http,
        url,
        fold_db::sync::auth::SyncAuth::ApiKey(api_key),
    )
    .without_db_auto_claim();
    let identity = plan.identity();
    if let Some(bytes) = read_small_regular(&home.join(COMMITTED_FILE), 16_384)? {
        let recorded: RescueS0Pointer =
            serde_json::from_slice(&bytes).map_err(|_| "invalid local S0 rescue commit receipt")?;
        let current = auth
            .rescue_s0_get(&identity.manifest_sha256)
            .await
            .map_err(|error| format!("read committed S0 rescue pointer: {error}"))?;
        if current != recorded || current.store_uuid != plan.manifest.store_uuid {
            return Err("local and cloud S0 rescue commit receipts disagree".into());
        }
        print_report(&plan, candidates.len(), true, json);
        return Ok(());
    }

    let writer_report = collect_writer_inventory(home, &plan.db_hash, &auth, &s3).await?;
    print_writer_inventory(&writer_report, json);
    save_writer_inventory_proof(home, &plan, writer_report)?;

    let prepared = auth
        .rescue_s0_prepare(&identity, &plan.manifest.store_uuid)
        .await
        .map_err(|error| format!("prepare S0 rescue hold: {error}"))?;
    wait_until(prepared.ready_after_unix_secs, wait, "hold drain").await?;

    for (index, (prefix, chunks)) in pages.iter().enumerate() {
        upload_page_chunks(home, &plan, prefix, chunks, &candidates, &auth, &s3)
            .await
            .map_err(|error| format!("S0 rescue page {prefix}: {error}"))?;
        if (index + 1) % 16 == 0 || index + 1 == pages.len() {
            eprintln!("S0 rescue pages: {}/{}", index + 1, pages.len());
        }
    }

    let manifest_bytes = plan.manifest_bytes()?;
    let manifest_presign = auth
        .rescue_s0_presign_manifest_upload(
            &identity,
            &plan.manifest.store_uuid,
            manifest_bytes.len() as u64,
        )
        .await
        .map_err(|error| format!("request S0 rescue manifest upload: {error}"))?;
    if !manifest_presign.already_present {
        let signed = manifest_presign
            .url
            .ok_or("S0 rescue manifest upload URL is missing")?;
        s3.upload_rescue_bytes(&signed, manifest_bytes)
            .await
            .map_err(|error| format!("upload S0 rescue manifest: {error}"))?;
    }
    auth.rescue_s0_descriptor_put(&identity, &plan.descriptor_bytes(&key)?)
        .await
        .map_err(|error| format!("store encrypted S0 recovery descriptor: {error}"))?;

    let committed = loop {
        match auth
            .rescue_s0_commit(
                &identity,
                &plan.manifest.store_uuid,
                plan.manifest.epoch,
                plan.manifest.counter,
            )
            .await
            .map_err(|error| format!("commit immutable S0 rescue pointer: {error}"))?
        {
            RescueS0CommitOutcome::Committed(value) => break value.rescue,
            RescueS0CommitOutcome::Wait {
                ready_after_unix_secs,
                ..
            } => wait_until(ready_after_unix_secs, wait, "upload URL expiry").await?,
        }
    };
    let exact = auth
        .rescue_s0_get(&identity.manifest_sha256)
        .await
        .map_err(|error| format!("read committed S0 rescue pointer: {error}"))?;
    if exact != committed {
        return Err("S0 rescue pointer changed after commit".into());
    }
    let receipt_bytes = serde_json::to_vec(&exact)
        .map_err(|error| format!("encode local S0 rescue receipt: {error}"))?;
    save_once(&home.join(COMMITTED_FILE), &receipt_bytes)?;
    print_report(&plan, candidates.len(), true, json);
    Ok(())
}

#[path = "cloud_rescue_publish/writer_inventory_proof.rs"]
mod writer_inventory_proof;
