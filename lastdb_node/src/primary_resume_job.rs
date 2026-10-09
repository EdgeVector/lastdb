//! Durable owner job for a primary-authoritative cloud resume.

use crate::cloud;
use crate::host::Host;
use fold_db::sync::engine::{PrimaryResumeCut, PrimaryResumeLogInventory};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

const RECEIPT_FILE: &str = ".cloud_primary_resume_job_v1.json";
const MAX_RECEIPT_BYTES: u64 = 64 * 1024;

mod finish;
mod restore_receipt;
pub(crate) use finish::finish;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u32,
    job_id: String,
    process_start_ts: u64,
    #[serde(default)]
    fresh_from_local: bool,
    #[serde(default)]
    accept_local_damage: bool,
    phase: String,
    cut: Option<PrimaryResumeCut>,
    after: Option<PrimaryResumeLogInventory>,
    error: Option<String>,
}

impl Receipt {
    fn new(process_start_ts: u64, fresh_from_local: bool, accept_local_damage: bool) -> Self {
        Self {
            version: 1,
            job_id: uuid::Uuid::new_v4().to_string(),
            process_start_ts,
            fresh_from_local,
            accept_local_damage,
            phase: "accepted".into(),
            cut: None,
            after: None,
            error: None,
        }
    }

    fn view(&self, home: &Path, running: bool) -> serde_json::Value {
        let marker = cloud::cloud_resume_required_path(home).exists();
        let state = if (self.phase == "complete" || self.phase == "finish_verified") && !marker {
            "complete"
        } else if self.phase == "failed" {
            "failed"
        } else if self.phase == "verified_backup" {
            "verified_backup"
        } else {
            "pending"
        };
        serde_json::json!({
            "state": state,
            "phase": if !running && state == "pending" { "interrupted" } else { self.phase.as_str() },
            "job_id": self.job_id,
            "resume_required": marker,
            "fresh_from_local": self.fresh_from_local,
            "accept_local_damage": self.accept_local_damage,
            "data_completeness": if self.accept_local_damage {
                "owner_accepted_local_damage"
            } else if self.fresh_from_local {
                "local_files_only"
            } else {
                "prior_cloud_and_local"
            },
            "sync_enabled": !marker && state == "complete",
            "error": self.error,
            "cut": self.cut.as_ref().map(|cut| &cut.manifest),
            "frontier": self.cut.as_ref().map(|cut| cut.writer_frontier),
            "after": self.after,
        })
    }

    fn accepts_restored_manifest(&self, manifest_sha256: &str) -> bool {
        matches!(
            self.phase.as_str(),
            "verified_backup" | "finish_verified" | "failed"
        ) && self.after.is_some()
            && self.cut.as_ref().is_some_and(|cut| {
                cut.manifest
                    .manifest_sha256
                    .eq_ignore_ascii_case(manifest_sha256)
            })
    }
}

fn receipt_path(home: &Path) -> PathBuf {
    home.join(RECEIPT_FILE)
}

fn read_receipt(home: &Path) -> Result<Option<Receipt>, String> {
    let path = receipt_path(home);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read primary resume job metadata: {error}")),
    };
    if !metadata.file_type().is_file() || metadata.len() > MAX_RECEIPT_BYTES {
        return Err("primary resume job receipt is not a small regular file".into());
    }
    let bytes = fs::read(&path).map_err(|error| format!("read primary resume job: {error}"))?;
    let receipt: Receipt = serde_json::from_slice(&bytes)
        .map_err(|error| format!("decode primary resume job: {error}"))?;
    if receipt.version != 1 || uuid::Uuid::parse_str(&receipt.job_id).is_err() {
        return Err("primary resume job receipt has invalid identity".into());
    }
    Ok(Some(receipt))
}

fn write_receipt(home: &Path, receipt: &Receipt) -> Result<(), String> {
    let bytes = serde_json::to_vec(receipt)
        .map_err(|error| format!("encode primary resume job: {error}"))?;
    if bytes.len() as u64 > MAX_RECEIPT_BYTES {
        return Err("primary resume job receipt exceeds its size limit".into());
    }
    let mut temp = tempfile::NamedTempFile::new_in(home)
        .map_err(|error| format!("create primary resume job receipt: {error}"))?;
    temp.write_all(&bytes)
        .and_then(|()| temp.as_file().sync_all())
        .map_err(|error| format!("sync primary resume job receipt: {error}"))?;
    temp.persist(receipt_path(home))
        .map_err(|error| format!("place primary resume job receipt: {}", error.error))?;
    File::open(home)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| format!("sync primary resume job directory: {error}"))
}

pub(crate) fn status(host: &Host) -> Result<serde_json::Value, String> {
    let marker = cloud::cloud_resume_required_path(&host.home).exists();
    let running = host.primary_resume_job_running.load(Ordering::Acquire);
    match read_receipt(&host.home)? {
        Some(receipt) => Ok(receipt.view(&host.home, running)),
        None => Ok(serde_json::json!({
            "state": if marker { "pending" } else { "not_required" },
            "phase": if marker && cloud::cloud_sync_file_state(&host.home) == "on" {
                "ready_for_job"
            } else if marker { "pending_restart" } else { "none" },
            "resume_required": marker,
            "sync_enabled": false,
        })),
    }
}

pub(crate) fn start(
    host: &Host,
    fresh_from_local: bool,
    accept_local_damage: bool,
) -> Result<serde_json::Value, String> {
    if accept_local_damage && !fresh_from_local {
        return Err("local damage acceptance requires fresh-from-local mode".into());
    }
    if cloud::cloud_sync_file_state(&host.home) != "on"
        || !cloud::cloud_resume_required_path(&host.home).exists()
    {
        return Err("primary resume requires an active config and durable resume marker".into());
    }
    let engine = host
        .db
        .sync_engine()
        .ok_or("primary resume requires a backup-only sync engine after restart")?;
    if !engine.is_backup_only_mode() {
        return Err("primary resume requires a backup-only sync engine".into());
    }
    if host
        .primary_resume_job_running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return status(host);
    }
    let mut receipt = match read_receipt(&host.home) {
        Ok(Some(receipt)) => receipt,
        Ok(None) => Receipt::new(host.process_start_ts, fresh_from_local, accept_local_damage),
        Err(error) => {
            host.primary_resume_job_running
                .store(false, Ordering::Release);
            return Err(error);
        }
    };
    if receipt.fresh_from_local != fresh_from_local
        || receipt.accept_local_damage != accept_local_damage
    {
        host.primary_resume_job_running
            .store(false, Ordering::Release);
        return Err("primary resume mode differs from the durable job".into());
    }
    if receipt.phase == "verified_backup" && receipt.cut.is_some() && receipt.after.is_some() {
        host.primary_resume_job_running
            .store(false, Ordering::Release);
        return Ok(receipt.view(&host.home, false));
    }
    receipt.process_start_ts = host.process_start_ts;
    receipt.phase = "accepted".into();
    receipt.error = None;
    if let Err(error) = write_receipt(&host.home, &receipt) {
        host.primary_resume_job_running
            .store(false, Ordering::Release);
        return Err(error);
    }
    let view = receipt.view(&host.home, true);
    let home = host.home.clone();
    let db = Arc::clone(&host.db);
    let running = Arc::clone(&host.primary_resume_job_running);
    // This process-lifetime job owns a runtime. A disconnected UDS request
    // cannot cancel it, and the durable cut survives a daemon restart.
    if let Err(error) = std::thread::Builder::new()
        .name("lastdb-primary-resume".into())
        .spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("start primary resume runtime: {error}"))
                .and_then(|runtime| runtime.block_on(run(&home, db.as_ref(), &mut receipt)));
            if let Err(error) = result {
                receipt.phase = "failed".into();
                receipt.error = Some(error);
                let _ = write_receipt(&home, &receipt);
            }
            running.store(false, Ordering::Release);
        })
    {
        host.primary_resume_job_running
            .store(false, Ordering::Release);
        return Err(format!("start primary resume job: {error}"));
    }
    Ok(view)
}

trait PrimaryResumeOps {
    async fn recover(&self, cut: &PrimaryResumeCut) -> Result<PrimaryResumeLogInventory, String>;
    async fn prepare(
        &self,
        fresh_from_local: bool,
        accept_local_damage: bool,
    ) -> Result<PrimaryResumeCut, String>;
    async fn publish(&self, cut: &PrimaryResumeCut) -> Result<PrimaryResumeLogInventory, String>;
}

impl PrimaryResumeOps for fold_db::fold_db_core::FoldDB {
    async fn recover(&self, cut: &PrimaryResumeCut) -> Result<PrimaryResumeLogInventory, String> {
        self.recover_primary_authoritative_cloud_resume(cut).await
    }

    async fn prepare(
        &self,
        fresh_from_local: bool,
        accept_local_damage: bool,
    ) -> Result<PrimaryResumeCut, String> {
        self.prepare_primary_authoritative_cloud_resume(fresh_from_local, accept_local_damage)
            .await
    }

    async fn publish(&self, cut: &PrimaryResumeCut) -> Result<PrimaryResumeLogInventory, String> {
        self.publish_primary_authoritative_cloud_resume(cut)
            .await
            .map(|(_, after)| after)
    }
}

async fn run<O: PrimaryResumeOps>(
    home: &Path,
    ops: &O,
    receipt: &mut Receipt,
) -> Result<(), String> {
    if let Some(cut) = receipt.cut.clone() {
        let after = match ops.recover(&cut).await {
            Ok(after) => after,
            Err(recovery_error) => {
                // A retry may still own the exact held cut in this process.
                // After a restart, the lost target makes this path fail closed;
                // a saved intent must never silently produce a different cut.
                let after = ops
                    .publish(&cut)
                    .await
                    .map_err(|publish_error| {
                        format!(
                            "saved primary resume cut could not recover or publish exactly; recovery: {recovery_error}; publication: {publish_error}"
                        )
                    })?;
                after
            }
        };
        receipt.after = Some(after);
        receipt.phase = "verified_backup".into();
        receipt.error = None;
        write_receipt(home, receipt)?;
        return Ok(());
    }
    let cut = ops
        .prepare(receipt.fresh_from_local, receipt.accept_local_damage)
        .await?;
    receipt.cut = Some(cut.clone());
    receipt.after = None;
    receipt.phase = "cut_prepared".into();
    write_receipt(home, receipt)?;
    let after = ops.publish(&cut).await?;
    receipt.after = Some(after);
    receipt.phase = "verified_backup".into();
    receipt.error = None;
    write_receipt(home, receipt)?;
    Ok(())
}

async fn run_finish(
    home: &Path,
    db: &fold_db::fold_db_core::FoldDB,
    receipt: &mut Receipt,
) -> Result<(), String> {
    let cut = receipt
        .cut
        .as_ref()
        .ok_or("finish lost its durable primary resume cut")?;
    let after = db.recover_primary_authoritative_cloud_resume(cut).await?;
    receipt.after = Some(after);
    receipt.phase = "finish_verified".into();
    write_receipt(home, receipt)?;
    if !cloud::cloud_resume_required_path(home).exists()
        || cloud::cloud_sync_file_state(home) != "on"
    {
        return Err("primary resume config or marker changed before final commit".into());
    }
    cloud::clear_cloud_resume_required(home)?;
    db.finish_primary_authoritative_cloud_resume().await?;
    receipt.phase = "complete".into();
    receipt.error = None;
    write_receipt(home, receipt)
}
