//! Durable append-only audit trail for retention policy changes.
//!
//! A retention policy decides which records are deleted for good, so every
//! change to one leaves a row: who asked, when, where the policy came from, and
//! the policy before and after. The row is written BEFORE the change
//! (`status: "applying"`): if it cannot be written, nothing changes. A second
//! row with the same `event_id` closes it (`"applied"` or `"failed"`). The log
//! is a line-per-event JSON file under the node home. It never rotates: a
//! policy changes rarely, and the trail must not lose its oldest rows.

use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use fold_db::schema::SchemaRetentionPolicy;
use serde::{Deserialize, Serialize};

pub const RETENTION_AUDIT_LOG_REL: &str = "logs/retention-audit.jsonl";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionAuditEvent {
    pub event_id: String,
    pub occurred_at: String,
    /// The route that asked: `apps_declare_schema` or `db_schema_retention`.
    pub route: String,
    pub caller: String,
    pub owner_user_id: String,
    /// `manifest_apply`, `anchor_move`, `operator_set` or `operator_clear`.
    pub action: String,
    /// The app that declared a manifest retention, when one did.
    pub app_id: Option<String>,
    /// The schema name the caller used.
    pub schema: String,
    /// The schema that holds the policy.
    pub anchor: String,
    /// `manifest` or `operator`: who the policy in force afterwards belongs to.
    pub source: String,
    /// The anchor the policy came from, for an `anchor_move`.
    pub moved_from: Option<String>,
    /// True when the caller confirmed that existing records may expire.
    pub confirmed_expire_existing: bool,
    /// The policy in force before the change.
    pub previous: Option<SchemaRetentionPolicy>,
    /// The policy in force after the change.
    pub current: Option<SchemaRetentionPolicy>,
    /// `applying` (written before the change), then `applied` or `failed`.
    pub status: String,
    /// Why a change failed, on a `failed` row.
    pub error: Option<String>,
}

impl RetentionAuditEvent {
    #[must_use]
    pub fn new(
        route: &str,
        caller: &str,
        owner_user_id: &str,
        action: &str,
        schema: &str,
        anchor: &str,
    ) -> Self {
        let now = chrono::Utc::now();
        Self {
            event_id: format!(
                "retention-{}-{}",
                now.timestamp_micros(),
                std::process::id()
            ),
            occurred_at: now.to_rfc3339(),
            route: route.to_string(),
            caller: caller.to_string(),
            owner_user_id: owner_user_id.to_string(),
            action: action.to_string(),
            app_id: None,
            schema: schema.to_string(),
            anchor: anchor.to_string(),
            source: String::new(),
            moved_from: None,
            confirmed_expire_existing: false,
            previous: None,
            current: None,
            status: "applying".to_string(),
            error: None,
        }
    }

    /// The row that closes this event: same `event_id`, new time and status.
    #[must_use]
    pub fn closed(&self, result: Result<(), &str>) -> Self {
        let mut row = self.clone();
        row.occurred_at = chrono::Utc::now().to_rfc3339();
        match result {
            Ok(()) => row.status = "applied".to_string(),
            Err(error) => {
                row.status = "failed".to_string();
                row.error = Some(error.to_string());
            }
        }
        row
    }

    /// Fill `source` from the policy in force after the change.
    #[must_use]
    pub fn with_policies(
        mut self,
        previous: Option<SchemaRetentionPolicy>,
        current: Option<SchemaRetentionPolicy>,
    ) -> Self {
        self.source = current
            .as_ref()
            .map_or("none", |policy| policy.source.as_str())
            .to_string();
        self.previous = previous;
        self.current = current;
        self
    }
}

#[must_use]
pub fn path_for_home(home: &Path) -> PathBuf {
    home.join(RETENTION_AUDIT_LOG_REL)
}

/// Append one event. The row is on disk when this returns `Ok`.
pub fn append(home: &Path, event: &RetentionAuditEvent) -> Result<(), String> {
    let path = path_for_home(home);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create retention audit directory {}: {e}", parent.display()))?;
    }
    let mut line = serde_json::to_string(event)
        .map_err(|e| format!("serialize retention audit event: {e}"))?;
    line.push('\n');
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&path)
        .map_err(|e| format!("open retention audit log {}: {e}", path.display()))?;
    file.write_all(line.as_bytes())
        .and_then(|()| file.sync_data())
        .map_err(|e| format!("append retention audit log {}: {e}", path.display()))
}
