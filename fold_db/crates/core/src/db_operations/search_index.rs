//! Search-plane outbox delivery helpers and bounded queue policy.

use crate::schema::types::{KeyValue, Schema, SchemaError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tracing::debug;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexChangeKind {
    Upsert,
    Tombstone,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexChange {
    pub mutation_id: String,
    pub kind: IndexChangeKind,
    pub key_value: KeyValue,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub record_fields: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexChangeBatch {
    #[serde(default = "version_one", skip_serializing_if = "is_version_one")]
    pub format_version: u32,
    pub schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub searchable_fields: Option<HashSet<String>>,
    pub changes: Vec<IndexChange>,
}

fn version_one() -> u32 {
    1
}

fn is_version_one(v: &u32) -> bool {
    *v == 1
}

/// Inbox directory for a node home, by layout alone (no environment reads).
pub fn search_inbox_dir_for_home(home: &Path) -> PathBuf {
    home.join("apps/search/inbox")
}

fn resolve_search_inbox_dir() -> Option<PathBuf> {
    let env_str = |k| {
        std::env::var(k)
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
    };
    env_str("LASTDB_SEARCH_INBOX")
        .map(PathBuf::from)
        .or_else(|| env_str("LASTDB_HOME").map(|h| search_inbox_dir_for_home(Path::new(&h))))
}

/// Deliver one Search change batch to the configured or environment-resolved inbox.
pub(crate) fn deliver_outbox(
    inbox: Option<PathBuf>,
    batch: &IndexChangeBatch,
) -> Result<(), SchemaError> {
    if !queue_policy::enabled() {
        return Ok(());
    }
    let Some(inbox) = inbox.or_else(resolve_search_inbox_dir) else {
        return Ok(());
    };
    let name = format!(
        "{}_{}.json",
        crate::clock::unix_millis(),
        Uuid::new_v4().simple()
    );
    let path = inbox.join(&name);
    let body = serde_json::to_vec_pretty(batch)
        .map_err(|e| SchemaError::InvalidData(format!("search outbox serialize: {e}")))?;
    let delivered = queue_policy::deliver(&inbox, &name, &body).map_err(|e| {
        SchemaError::InvalidData(format!("search outbox delivery {}: {e}", path.display()))
    })?;
    debug!(
        target: "search_outbox", path = %path.display(), schema = %batch.schema_name,
        changes = batch.changes.len(), delivered, "Search batch delivery decision"
    );
    Ok(())
}

pub(crate) fn searchable_fields(schema: &Schema) -> Option<HashSet<String>> {
    (!schema.field_classifications.is_empty()).then(|| {
        schema
            .field_classifications
            .iter()
            .filter(|(_, c)| {
                !c.iter().any(|t| {
                    ["secret", "no_index", "no-index"]
                        .iter()
                        .any(|skip| t.eq_ignore_ascii_case(skip))
                }) && c.iter().any(|t| t.eq_ignore_ascii_case("word"))
            })
            .map(|(name, _)| name.clone())
            .collect()
    })
}

pub(crate) fn inbox_fields(
    fields: &HashMap<String, Value>,
    searchable: Option<&HashSet<String>>,
) -> HashMap<String, Value> {
    fields
        .iter()
        .filter(|(name, _)| searchable.is_none_or(|allow| allow.contains(*name)))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

pub mod queue_policy {
    use std::fs::{self, File, OpenOptions};
    use std::io::{self, Write};
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    const REBUILD_REQUIRED: &str = ".rebuild-required";
    const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;
    const MAX_FILES: usize = 4096;
    static CLI_ENABLED: AtomicBool = AtomicBool::new(false);

    pub fn enable_for_process() {
        CLI_ENABLED.store(true, Ordering::Relaxed);
    }

    pub fn enabled() -> bool {
        CLI_ENABLED.load(Ordering::Relaxed)
            || std::env::var("LASTDB_SEARCH_ENABLED")
                .is_ok_and(|v| matches!(v.trim(), "1" | "true" | "on"))
    }

    fn max_bytes() -> u64 {
        crate::env_flag::var_or("LASTDB_SEARCH_QUEUE_MAX_BYTES", DEFAULT_MAX_BYTES)
            .clamp(64 * 1024, 1024 * 1024 * 1024)
    }

    pub fn status(inbox: &Path) -> serde_json::Value {
        let marker = inbox.join(REBUILD_REQUIRED).try_exists();
        serde_json::json!({
            "enabled": enabled(), "max_batch_bytes": max_bytes(), "max_batch_files": MAX_FILES,
            "rebuild_required": marker.as_ref().ok(),
            "status_error": marker.err().map(|e| e.to_string()),
        })
    }

    pub fn prepare(inbox: &Path) -> io::Result<()> {
        if !enabled() && inbox.try_exists()? {
            let _lock = producer_lock(inbox)?;
            mark_rebuild(inbox, "search_disabled")?;
        }
        Ok(())
    }

    fn producer_lock(inbox: &Path) -> io::Result<File> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(inbox.join(".producer.lock"))?;
        file.lock()?;
        Ok(file)
    }

    fn mark_rebuild(inbox: &Path, reason: &str) -> io::Result<()> {
        if inbox.join(REBUILD_REQUIRED).try_exists()? {
            return Ok(());
        }
        let body = serde_json::to_vec(&serde_json::json!({
            "format_version": 1, "rebuild_required": true, "reason": reason,
            "max_batch_bytes": max_bytes(), "max_batch_files": MAX_FILES,
        }))?;
        atomic_write(inbox, REBUILD_REQUIRED, &body)?;
        tracing::warn!(
            reason,
            "Search delivery paused; rebuild Search from LastDB before resuming the queue"
        );
        Ok(())
    }

    fn fits(inbox: &Path, incoming: u64) -> io::Result<bool> {
        let (mut bytes, mut batches, cap) = (incoming, 1usize, max_bytes());
        let deadline = Instant::now() + Duration::from_secs(1);
        for (entries, entry) in fs::read_dir(inbox)?.enumerate() {
            if entries >= MAX_FILES + 16 || Instant::now() >= deadline {
                return Ok(false);
            }
            let entry = entry?;
            let info = match entry.path().symlink_metadata() {
                Ok(info) => info,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            if !info.is_file() {
                return Ok(false);
            }
            if entry.file_name() != ".producer.lock" {
                batches += 1;
                bytes = bytes.saturating_add(info.len());
            }
            if batches > MAX_FILES || bytes > cap {
                return Ok(false);
            }
        }
        Ok(bytes <= cap)
    }

    pub(in crate::db_operations::search_index) fn deliver(
        inbox: &Path,
        name: &str,
        body: &[u8],
    ) -> io::Result<bool> {
        fs::create_dir_all(inbox)?;
        let _lock = producer_lock(inbox)?;
        if inbox.join(REBUILD_REQUIRED).try_exists()? {
            return Ok(false);
        }
        if !fits(inbox, body.len() as u64)? {
            mark_rebuild(inbox, "queue_limit")?;
            return Ok(false);
        }
        atomic_write(inbox, name, body)?;
        Ok(true)
    }

    fn atomic_write(inbox: &Path, name: &str, body: &[u8]) -> io::Result<()> {
        let mut temp = tempfile::NamedTempFile::new_in(inbox)?;
        temp.write_all(body)?;
        temp.as_file().sync_all()?;
        temp.persist_noclobber(inbox.join(name))
            .map_err(|e| e.error)?;
        File::open(inbox)?.sync_all()
    }
}
