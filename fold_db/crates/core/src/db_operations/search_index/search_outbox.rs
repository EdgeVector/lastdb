//! Host delivery of [`IndexChangeBatch`] to the first-party Search app.
//!
//! Writes one JSON file per batch under:
//!   `{LASTDB_HOME|FOLDDB_HOME}/apps/search/inbox/{uuid}.json`
//! or `LASTDB_SEARCH_INBOX` when set.
//!
//! The Search app (`https://github.com/EdgeVector/search`) drains this inbox into a regenerable
//! local index. No FastEmbed/ONNX on this path — default Mini write stays thin.

use super::sink::{IndexChangeBatch, IndexSink};
use crate::schema::SchemaError;
use async_trait::async_trait;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, warn};
use uuid::Uuid;

/// Inbox directory for a node home, by layout alone (no environment reads).
pub fn search_inbox_dir_for_home(home: &Path) -> PathBuf {
    home.join("apps").join("search").join("inbox")
}

/// Explicit `LASTDB_SEARCH_INBOX` override, when set to a non-empty path.
fn search_inbox_override_from_env() -> Option<PathBuf> {
    let explicit = std::env::var("LASTDB_SEARCH_INBOX").ok()?;
    let p = PathBuf::from(explicit.trim());
    (!p.as_os_str().is_empty()).then_some(p)
}

/// Resolve the Search app inbox directory for host delivery.
///
/// Environment-driven: for the daemon and CLI entry points, where the node home
/// is only known through `LASTDB_HOME` / `FOLDDB_HOME`. Callers that already
/// hold the home path must use [`resolve_search_inbox_dir_for_home`] instead —
/// it does not depend on process-global state.
pub fn resolve_search_inbox_dir() -> Option<PathBuf> {
    if let Some(explicit) = search_inbox_override_from_env() {
        return Some(explicit);
    }
    let home = std::env::var("LASTDB_HOME")
        .or_else(|_| std::env::var("FOLDDB_HOME"))
        .ok()?;
    let home = home.trim();
    if home.is_empty() {
        return None;
    }
    Some(search_inbox_dir_for_home(Path::new(home)))
}

/// Resolve the Search inbox for an **explicitly named** home.
///
/// Same precedence as [`resolve_search_inbox_dir`] would give with
/// `LASTDB_HOME=home` (an explicit `LASTDB_SEARCH_INBOX` still wins), but the
/// home travels as an argument instead of through the process environment. This
/// is what lets concurrent callers target different homes: nothing here writes
/// to `std::env`, so two in-flight rebuilds cannot clobber each other's inbox.
pub fn resolve_search_inbox_dir_for_home(home: &Path) -> PathBuf {
    search_inbox_override_from_env().unwrap_or_else(|| search_inbox_dir_for_home(home))
}

/// File-backed [`IndexSink`] that delivers batches to the Search app inbox.
pub struct SearchOutboxSink {
    inbox: PathBuf,
}

impl SearchOutboxSink {
    pub fn from_env() -> Option<Self> {
        let inbox = resolve_search_inbox_dir()?;
        Some(Self { inbox })
    }

    pub fn new(inbox: PathBuf) -> Self {
        Self { inbox }
    }
}

#[async_trait]
impl IndexSink for SearchOutboxSink {
    async fn apply_change_batch(&self, batch: IndexChangeBatch) -> Result<(), SchemaError> {
        fs::create_dir_all(&self.inbox).map_err(|e| {
            SchemaError::InvalidData(format!("search outbox mkdir {}: {e}", self.inbox.display()))
        })?;
        let name = format!("{}_{}.json", chrono_like_stamp(), Uuid::new_v4().simple());
        let path = self.inbox.join(name);
        let tmp = path.with_extension("json.tmp");
        let body = serde_json::to_vec_pretty(&batch)
            .map_err(|e| SchemaError::InvalidData(format!("search outbox serialize: {e}")))?;
        fs::write(&tmp, &body).map_err(|e| {
            SchemaError::InvalidData(format!("search outbox write {}: {e}", tmp.display()))
        })?;
        fs::rename(&tmp, &path).map_err(|e| {
            SchemaError::InvalidData(format!("search outbox rename {}: {e}", path.display()))
        })?;
        debug!(
            target: "search_outbox",
            path = %path.display(),
            schema = %batch.schema_name,
            changes = batch.changes.len(),
            "delivered IndexChangeBatch to Search inbox"
        );
        Ok(())
    }
}

fn chrono_like_stamp() -> String {
    // Avoid pulling chrono solely for a filename; millis since epoch is enough.
    let ms = crate::clock::unix_millis();
    format!("{ms}")
}

/// Best-effort deliver `batch` to the Search outbox. Never fails the write path.
pub async fn deliver_search_outbox_best_effort(batch: IndexChangeBatch) {
    let Some(sink) = SearchOutboxSink::from_env() else {
        return;
    };
    if let Err(e) = sink.apply_change_batch(batch).await {
        warn!("Search outbox delivery failed: {e}");
    }
}
