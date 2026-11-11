//! Host delivery of [`IndexChangeBatch`] to the first-party Search app.
//!
//! Writes one JSON file per batch under `{LASTDB_HOME}/apps/search/inbox/{uuid}.json`
//! or `LASTDB_SEARCH_INBOX` when set. Delivery is off unless `--enable-search`
//! or `LASTDB_SEARCH_ENABLED=1` enables it.

use super::sink::IndexChangeBatch;
use crate::schema::SchemaError;
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

/// File-backed outbox that delivers batches to the Search app inbox.
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
    pub(crate) fn deliver(
        inbox: Option<PathBuf>,
        batch: &IndexChangeBatch,
    ) -> Result<(), SchemaError> {
        if !super::queue_policy::enabled() {
            return Ok(());
        }
        if let Some(sink) = inbox.map(Self::new).or_else(Self::from_env) {
            sink.write_batch(batch)?;
        }
        Ok(())
    }

    fn write_batch(&self, batch: &IndexChangeBatch) -> Result<(), SchemaError> {
        let name = format!(
            "{}_{}.json",
            crate::clock::unix_millis(),
            Uuid::new_v4().simple()
        );
        let path = self.inbox.join(&name);
        let body = serde_json::to_vec_pretty(batch)
            .map_err(|e| SchemaError::InvalidData(format!("search outbox serialize: {e}")))?;
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
