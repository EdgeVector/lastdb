//! Off-hot-path Search rebuild: page product records into IndexChangeBatch
//! files for the Search app (LastStore-backed index).
//!
//! **Never call from the mutation critical section.** Live writes use
//! [`crate::db_operations::search_index::deliver_search_outbox_best_effort`] only (single
//! batch). Full-corpus walks live here (restore / cold home / explicit rebuild).

use super::sink::{IndexChange, IndexChangeBatch, IndexChangeKind};
use crate::atom::is_tombstone_value;
use crate::db_operations::search_index::{
    resolve_search_inbox_dir, resolve_search_inbox_dir_for_home,
};
use crate::db_operations::DbOperations;
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::Schema;
use crate::schema::SchemaError;
use crate::storage::{LastStoreNamespacedStore, NamespacedStore};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

/// Cursor for iterative Search rebuild over schemas/records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchRebuildCursor {
    /// Index into sorted schema name list.
    pub schema_idx: usize,
    /// Index into sorted record keys within the current schema.
    pub record_idx: usize,
}

/// One page of rebuild work.
#[derive(Debug, Clone)]
pub struct SearchRebuildPage {
    /// Batches to apply (usually one per schema slice).
    pub batches: Vec<IndexChangeBatch>,
    /// Next cursor, or None when exhausted.
    pub next: Option<SearchRebuildCursor>,
}

/// Report after a full rebuild emit.
#[derive(Debug, Clone, Default)]
pub struct SearchRebuildReport {
    /// Pages emitted.
    pub pages: usize,
    /// Batches written.
    pub batches: usize,
    /// Individual changes written.
    pub changes: usize,
    /// Inbox directory used.
    pub inbox: PathBuf,
}

/// Searchable field names for a schema (mirrors mutation_manager native-index policy).
fn searchable_fields_for_schema(schema: &Schema) -> Option<HashSet<String>> {
    if schema.field_classifications.is_empty() {
        return None;
    }
    Some(
        schema
            .field_classifications
            .iter()
            .filter(|(_, classifications)| {
                !classifications.iter().any(|c| {
                    c.eq_ignore_ascii_case("secret")
                        || c.eq_ignore_ascii_case("no_index")
                        || c.eq_ignore_ascii_case("no-index")
                })
            })
            .filter(|(_, classifications)| {
                classifications
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case("word"))
            })
            .map(|(field_name, _)| field_name.clone())
            .collect(),
    )
}

/// Collect all searchable field values for one schema into key → fields map.
async fn collect_schema_records(
    ops: &DbOperations,
    _schema_name: &str,
    schema: &mut Schema,
) -> Result<Vec<(KeyValue, HashMap<String, Value>)>, SchemaError> {
    let searchable = searchable_fields_for_schema(schema);
    if searchable.as_ref().is_some_and(HashSet::is_empty) {
        return Ok(vec![]);
    }

    let mut records: HashMap<KeyValue, HashMap<String, Value>> = HashMap::new();
    let mut field_names: Vec<String> = schema.runtime_fields.keys().cloned().collect();
    field_names.sort();

    for field_name in field_names {
        if searchable
            .as_ref()
            .is_some_and(|fields| !fields.contains(&field_name))
        {
            continue;
        }
        let Some(field) = schema.runtime_fields.get_mut(&field_name) else {
            continue;
        };
        field.refresh_from_db(ops).await?;
        for key in field.get_all_keys() {
            let Some(atom_uuid) = field.current_atom_uuid(&key) else {
                continue;
            };
            // Slot in scope — name the partition instead of paying the
            // locator hop on every row of a full index rebuild.
            let hint = field.partition_hint(&key);
            let Some(atom) = ops
                .atoms()
                .get_atom_by_uuid_in_partition(&atom_uuid, hint.as_ref(), None)
                .await?
            else {
                continue;
            };
            if is_tombstone_value(atom.content()) {
                continue;
            }
            records
                .entry(key)
                .or_default()
                .insert(field_name.clone(), atom.content().clone());
        }
    }

    let mut batch: Vec<_> = records.into_iter().collect();
    batch.sort_by(|(left, _), (right, _)| {
        left.hash
            .as_deref()
            .unwrap_or("")
            .cmp(right.hash.as_deref().unwrap_or(""))
            .then_with(|| {
                left.range
                    .as_deref()
                    .unwrap_or("")
                    .cmp(right.range.as_deref().unwrap_or(""))
            })
    });
    Ok(batch)
}

/// Page product records into IndexChangeBatches for Search rebuild.
///
/// This is the LastDB iteration API for Search: callers advance `cursor`
/// until `next` is None. Does not touch the mutation write path.
pub async fn page_searchable_records_for_rebuild(
    ops: &DbOperations,
    cursor: Option<SearchRebuildCursor>,
    page_size: usize,
) -> Result<SearchRebuildPage, SchemaError> {
    let page_size = page_size.max(1);
    let mut schemas: Vec<_> = ops.schemas().get_all_schemas().await?.into_iter().collect();
    schemas.sort_by(|(a, _), (b, _)| a.cmp(b));

    let cur = cursor.unwrap_or_default();
    if cur.schema_idx >= schemas.len() {
        return Ok(SearchRebuildPage {
            batches: vec![],
            next: None,
        });
    }

    // Load records for the current schema (per-schema collect; then slice).
    let (schema_name, mut schema) = schemas[cur.schema_idx].clone();
    let records = collect_schema_records(ops, &schema_name, &mut schema).await?;
    let searchable = searchable_fields_for_schema(&schema);

    if cur.record_idx >= records.len() {
        // Advance to next schema.
        let next = SearchRebuildCursor {
            schema_idx: cur.schema_idx + 1,
            record_idx: 0,
        };
        return Box::pin(page_searchable_records_for_rebuild(
            ops,
            Some(next),
            page_size,
        ))
        .await;
    }

    let end = (cur.record_idx + page_size).min(records.len());
    let slice = &records[cur.record_idx..end];
    let changes: Vec<IndexChange> = slice
        .iter()
        .map(|(key, fields)| IndexChange {
            mutation_id: format!("rebuild-{}", Uuid::new_v4()),
            kind: IndexChangeKind::Upsert,
            key_value: key.clone(),
            fields_and_values: fields.clone(),
        })
        .collect();

    let mut batches = Vec::new();
    if !changes.is_empty() {
        batches.push(IndexChangeBatch {
            schema_name: schema_name.clone(),
            searchable_fields: searchable,
            changes,
        });
    }

    let next = if end < records.len() {
        Some(SearchRebuildCursor {
            schema_idx: cur.schema_idx,
            record_idx: end,
        })
    } else if cur.schema_idx + 1 < schemas.len() {
        Some(SearchRebuildCursor {
            schema_idx: cur.schema_idx + 1,
            record_idx: 0,
        })
    } else {
        None
    };

    Ok(SearchRebuildPage { batches, next })
}

/// Write batches as JSON files into the Search inbox (host outbox layout).
pub fn write_batches_to_search_inbox(
    inbox: &Path,
    batches: &[IndexChangeBatch],
) -> Result<usize, SchemaError> {
    fs::create_dir_all(inbox).map_err(|e| {
        SchemaError::InvalidData(format!("search rebuild mkdir {}: {e}", inbox.display()))
    })?;
    let mut n = 0usize;
    for batch in batches {
        let name = format!(
            "rebuild_{}_{}.json",
            crate::clock::unix_millis(),
            Uuid::new_v4().simple()
        );
        let path = inbox.join(name);
        let body = serde_json::to_vec_pretty(batch)
            .map_err(|e| SchemaError::InvalidData(format!("search rebuild serialize: {e}")))?;
        fs::write(&path, body).map_err(|e| {
            SchemaError::InvalidData(format!("search rebuild write {}: {e}", path.display()))
        })?;
        n += 1;
    }
    Ok(n)
}

/// Full iterative rebuild emit: page all product records into Search inbox.
///
/// **Off hot path** — call from restore hooks, CLI, or background task only.
/// Never from `spawn_index_mutations`.
///
/// Resolves the inbox from the environment. Callers that already know the home
/// must use [`rebuild_search_outbox_into_inbox`] and pass the path explicitly.
pub async fn rebuild_search_outbox_from_source(
    ops: &DbOperations,
    page_size: usize,
) -> Result<SearchRebuildReport, SchemaError> {
    let inbox = resolve_search_inbox_dir().ok_or_else(|| {
        SchemaError::InvalidData(
            "Search rebuild requires LASTDB_HOME or LASTDB_SEARCH_INBOX".into(),
        )
    })?;
    rebuild_search_outbox_into_inbox(ops, page_size, &inbox).await
}

/// Full iterative rebuild emit into an **explicitly supplied** inbox.
///
/// The environment is never read or written here, so concurrent rebuilds
/// against different homes are independent.
pub async fn rebuild_search_outbox_into_inbox(
    ops: &DbOperations,
    page_size: usize,
    inbox: &Path,
) -> Result<SearchRebuildReport, SchemaError> {
    let inbox = inbox.to_path_buf();
    let mut report = SearchRebuildReport {
        inbox: inbox.clone(),
        ..Default::default()
    };
    let mut cursor: Option<SearchRebuildCursor> = None;
    loop {
        let page = page_searchable_records_for_rebuild(ops, cursor, page_size).await?;
        if page.batches.is_empty() && page.next.is_none() {
            break;
        }
        if !page.batches.is_empty() {
            report.pages += 1;
            let written = write_batches_to_search_inbox(&inbox, &page.batches)?;
            report.batches += written;
            report.changes += page.batches.iter().map(|b| b.changes.len()).sum::<usize>();
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    Ok(report)
}

/// Operable entry: open a LastDB **home** (Mini layout: LastStore under
/// `home/data`, Search inbox under `home/apps/search/inbox`), walk product
/// records, emit IndexChangeBatch files for the Search app.
///
/// The inbox is derived from `home` directly (see
/// [`resolve_search_inbox_dir_for_home`]) — this function does **not** publish
/// `home` to `LASTDB_HOME`. Writing that process-global was a data race: two
/// concurrent callers (the offline CLI, or simply two tests in one binary) would
/// last-writer-win each other, and a rebuild could then emit into — or read back
/// from — a home it was never given. Prefer when the daemon is stopped
/// (exclusive open) or against an ephemeral/restored home. Never call from the
/// mutation hot path.
pub async fn run_search_rebuild_for_home(
    home: &Path,
    page_size: usize,
) -> Result<SearchRebuildReport, SchemaError> {
    let inbox = resolve_search_inbox_dir_for_home(home);
    let data = home.join("data");
    fs::create_dir_all(&data)
        .map_err(|e| SchemaError::InvalidData(format!("search rebuild create data dir: {e}")))?;
    fs::create_dir_all(&inbox)
        .map_err(|e| SchemaError::InvalidData(format!("search rebuild create inbox: {e}")))?;

    let store = LastStoreNamespacedStore::open(&data)
        .map_err(|e| SchemaError::InvalidData(format!("open LastStore for search rebuild: {e}")))?;
    let ops = DbOperations::from_namespaced_store(Arc::new(store) as Arc<dyn NamespacedStore>)
        .await
        .map_err(|e| {
            SchemaError::InvalidData(format!("open DbOperations for search rebuild: {e}"))
        })?;
    rebuild_search_outbox_into_inbox(&ops, page_size, &inbox).await
}

/// Fixture / unit-test page source: prebuilt batches, not FoldDB.
#[derive(Debug, Clone)]
pub struct FixtureBatchSource {
    batches: Vec<IndexChangeBatch>,
    offset: usize,
}

impl FixtureBatchSource {
    /// Wrap batches for paged emission.
    pub fn new(batches: Vec<IndexChangeBatch>) -> Self {
        Self { batches, offset: 0 }
    }

    /// Next page of at most `limit` batches.
    pub fn next_page(&mut self, limit: usize) -> Vec<IndexChangeBatch> {
        if self.offset >= self.batches.len() || limit == 0 {
            return vec![];
        }
        let end = (self.offset + limit).min(self.batches.len());
        let page = self.batches[self.offset..end].to_vec();
        self.offset = end;
        page
    }
}
