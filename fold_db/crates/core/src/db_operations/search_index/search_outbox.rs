//! Host delivery of [`IndexChangeBatch`] to the first-party Search app.
//!
//! Writes one JSON file per batch under:
//!   `{LASTDB_HOME}/apps/search/inbox/{uuid}.json`
//! or `LASTDB_SEARCH_INBOX` when set.
//!
//! LastSeek drains this optional inbox into its regenerable local store.
//! Delivery is off unless `--enable-search` or `LASTDB_SEARCH_ENABLED=1` enables it.
//! No FastEmbed/ONNX runs on this path.

use super::sink::{IndexChangeBatch, IndexSink};
use crate::schema::SchemaError;
use async_trait::async_trait;
use laststore::commit_timing as timing;
use std::path::{Path, PathBuf};
use tracing::debug;
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
/// is only known through `LASTDB_HOME`. Callers that already
/// hold the home path must use [`resolve_search_inbox_dir_for_home`] instead —
/// it does not depend on process-global state.
pub fn resolve_search_inbox_dir() -> Option<PathBuf> {
    if let Some(explicit) = search_inbox_override_from_env() {
        return Some(explicit);
    }
    let home = std::env::var("LASTDB_HOME").ok()?;
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
        if !super::queue_policy::enabled() {
            return None;
        }
        let inbox = resolve_search_inbox_dir()?;
        Some(Self { inbox })
    }

    pub fn new(inbox: PathBuf) -> Self {
        Self { inbox }
    }

    /// Send one batch. A configured inbox wins. The env inbox is the fallback.
    /// No inbox sends nothing.
    pub(crate) async fn deliver(
        inbox: Option<PathBuf>,
        batch: IndexChangeBatch,
    ) -> Result<(), SchemaError> {
        let sink = inbox.map(Self::new).or_else(Self::from_env);
        if let Some(sink) = sink {
            sink.apply_change_batch(batch).await?;
        }
        Ok(())
    }

    fn write_batch(&self, batch: &IndexChangeBatch) -> Result<(), SchemaError> {
        timing::count("search_changes", batch.changes.len() as u64);
        let name = format!("{}_{}.json", chrono_like_stamp(), Uuid::new_v4().simple());
        let path = self.inbox.join(&name);
        let body = timing::during("search_json_encode_us", || serde_json::to_vec_pretty(batch))
            .map_err(|e| SchemaError::InvalidData(format!("search outbox serialize: {e}")))?;
        timing::count("search_json_bytes", body.len() as u64);
        let delivered = super::queue_policy::deliver(&self.inbox, &name, &body).map_err(|e| {
            SchemaError::InvalidData(format!("search outbox delivery {}: {e}", path.display()))
        })?;
        debug!(
            target: "search_outbox",
            path = %path.display(),
            schema = %batch.schema_name,
            changes = batch.changes.len(),
            delivered,
            "Search batch delivery decision"
        );
        Ok(())
    }
}

#[async_trait]
impl IndexSink for SearchOutboxSink {
    async fn apply_change_batch(&self, batch: IndexChangeBatch) -> Result<(), SchemaError> {
        if !super::queue_policy::enabled() {
            return Ok(());
        }
        let (result, report) = timing::measure(|| self.write_batch(&batch));
        if let Some(report) = report {
            timing::emit(
                "LASTDB_SNAPSHOT_TIMING",
                &serde_json::json!({
                    "kind": "search_outbox", "ok": result.is_ok(),
                    "started_us": report.started_us, "ended_us": report.ended_us,
                    "total_us": report.total_us, "covered_us": report.covered_us,
                    "unassigned_us": report.unassigned_us,
                    "phases_us": report.phases_us, "counts": report.counts,
                }),
            );
        }
        result
    }
}

fn chrono_like_stamp() -> String {
    // Avoid pulling chrono solely for a filename; millis since epoch is enough.
    let ms = crate::clock::unix_millis();
    format!("{ms}")
}
