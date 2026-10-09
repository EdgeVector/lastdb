//! Durable append-only audit trail for canonical app schema synchronization.

use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const SCHEMA_SYNC_AUDIT_LOG_REL: &str = "logs/schema-sync-audit.jsonl";
pub const DEFAULT_SCHEMA_SYNC_AUDIT_MAX_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaSyncAuditEvent {
    pub event_id: String,
    pub occurred_at: String,
    pub route: String,
    pub caller: String,
    pub owner_user_id: String,
    pub app_id: String,
    pub schema: String,
    pub proposal_identity_hash: Option<String>,
    pub intent: String,
    pub outcome: String,
    pub catalog_hashes: Vec<String>,
    pub predecessor_hashes: Vec<String>,
    pub mapper_count: usize,
    pub bind_eligible: bool,
    pub error: Option<String>,
}

impl SchemaSyncAuditEvent {
    #[must_use]
    pub fn new(route: &str, caller: &str, owner_user_id: &str, app_id: &str, schema: &str) -> Self {
        let now = chrono::Utc::now();
        Self {
            event_id: format!(
                "schema-sync-{}-{}",
                now.timestamp_micros(),
                std::process::id()
            ),
            occurred_at: now.to_rfc3339(),
            route: route.to_string(),
            caller: caller.to_string(),
            owner_user_id: owner_user_id.to_string(),
            app_id: app_id.to_string(),
            schema: schema.to_string(),
            proposal_identity_hash: None,
            intent: "catalog_sync".to_string(),
            outcome: "rejected".to_string(),
            catalog_hashes: Vec::new(),
            predecessor_hashes: Vec::new(),
            mapper_count: 0,
            bind_eligible: false,
            error: None,
        }
    }
}

#[must_use]
pub fn path_for_home(home: &Path) -> PathBuf {
    home.join(SCHEMA_SYNC_AUDIT_LOG_REL)
}

pub fn append(home: &Path, event: &SchemaSyncAuditEvent) -> Result<(), String> {
    let max_bytes = env_flag::var_parsed::<u64>("LASTDB_SCHEMA_SYNC_AUDIT_MAX_BYTES")
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SCHEMA_SYNC_AUDIT_MAX_BYTES);
    append_with_max_bytes(home, event, max_bytes)
}

fn append_with_max_bytes(
    home: &Path,
    event: &SchemaSyncAuditEvent,
    max_bytes: u64,
) -> Result<(), String> {
    let path = path_for_home(home);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            format!(
                "create schema-sync audit directory {}: {e}",
                parent.display()
            )
        })?;
    }
    if std::fs::metadata(&path).is_ok_and(|metadata| metadata.len() >= max_bytes) {
        let rotated = path.with_extension("jsonl.1");
        let _ = std::fs::remove_file(&rotated);
        std::fs::rename(&path, &rotated).map_err(|e| {
            format!(
                "rotate schema-sync audit log {} to {}: {e}",
                path.display(),
                rotated.display()
            )
        })?;
    }
    let mut line = serde_json::to_string(event)
        .map_err(|e| format!("serialize schema-sync audit event: {e}"))?;
    line.push('\n');
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&path)
        .map_err(|e| format!("open schema-sync audit log {}: {e}", path.display()))?;
    file.write_all(line.as_bytes())
        .map_err(|e| format!("append schema-sync audit log {}: {e}", path.display()))
}
