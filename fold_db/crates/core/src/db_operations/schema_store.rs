//! Schema domain store.
//!
//! Owns the storage namespaces for schemas, schema states, and
//! schema supersede-by mappings. External callers access schema
//! operations through this type via `DbOperations::schemas()`.

use crate::schema::{Schema, SchemaError, SchemaState};
use crate::storage::traits::KvStore;
use crate::storage::TypedKvStore;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Durable janitor target written before a catalog cut.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SchemaDropReceipt {
    pub identity: String,
    pub owner_app: Option<String>,
    pub field_molecule_uuids: Vec<String>,
    pub dropped_at_unix_ms: u64,
}

mod catalog_keys;
mod claims_retention;
mod key_format;
mod name_claims;
mod native_memory;
mod read;
mod read_snapshot;
mod replay_catalog;
mod replay_probe;
mod retention_backfill;
mod retention_format;
mod retention_layers;
mod retention_move;
mod retention_page;
mod retention_recheck;
mod retention_sharing;
mod retired_replay;
mod write;

pub(crate) use native_memory::{schema_bytes, snapshot_bytes, snapshot_sources};
pub(crate) use read_snapshot::SchemaReadSnapshot;
pub(crate) use retention_format::require_typed_range_retention;

/// Frozen conversion reads use the same exact retention-key layout as the node.
pub(crate) fn frozen_retention_keys(schema_name: &str) -> [String; 2] {
    [
        catalog_keys::retention_anchor_key(schema_name),
        catalog_keys::retention_policy_key(schema_name),
    ]
}
pub use replay_probe::ReplayAuthorityProbe;
pub(crate) use retired_replay::RetiredReplayProof;

use catalog_keys::*;

/// Domain store for schema-related persistence.
#[derive(Clone)]
pub struct SchemaStore {
    schemas_store: Arc<TypedKvStore<dyn KvStore>>,
    schema_states_store: Arc<TypedKvStore<dyn KvStore>>,
    superseded_by_store: Arc<TypedKvStore<dyn KvStore>>,
    /// Mini E2E content key. Catalog rows are plaintext-by-policy, but a
    /// mis-wired sealer wrote `ENC:` tips (2026-08-16). Query falls back to
    /// [`Self::get_schema`] on a cache miss; without this key that get would
    /// serde-fail and become HTTP 400. Same key as atom content seal.
    catalog_unwrap_key: Option<[u8; 32]>,
    runtime: Option<Arc<crate::storage::disk_pack::PackRuntime>>,
}

impl SchemaStore {
    pub(crate) fn new(
        schemas_store: Arc<TypedKvStore<dyn KvStore>>,
        schema_states_store: Arc<TypedKvStore<dyn KvStore>>,
        superseded_by_store: Arc<TypedKvStore<dyn KvStore>>,
        runtime: Option<Arc<crate::storage::disk_pack::PackRuntime>>,
    ) -> Self {
        Self {
            schemas_store,
            schema_states_store,
            superseded_by_store,
            catalog_unwrap_key: None,
            runtime,
        }
    }

    /// Use the Mini content key to open `ENC:` catalog tips on get/boot.
    pub(crate) fn with_catalog_unwrap_key(mut self, key: [u8; 32]) -> Self {
        self.catalog_unwrap_key = Some(key);
        self
    }

    /// Flush every schema-owned namespace to durable storage.
    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.schemas_store.inner().flush().await?;
        self.schema_states_store.inner().flush().await?;
        self.superseded_by_store.inner().flush().await?;
        Ok(())
    }

    /// Get all schemas.
    ///
    /// **Won't-undo:** a single unreadable durable row must not take down the
    /// whole catalog load. This runs on the boot path, and an aborting scan here
    /// does not degrade one schema — it fails `open_existing_store` outright, so
    /// the node exits with *"cannot open existing store — refusing to start
    /// fresh over existing data"* and cannot be started again by any binary.
    /// That is self-latching: the node cannot boot, so nothing can ever run the
    /// write that would replace the bad row. Tom's primary reached exactly that
    /// state on 2026-08-16 — a scribbled `BoardCards`/`Card` row left an
    /// 11-hour-old process as the only thing standing between him and an
    /// unbootable database, because it still held a good catalog in memory.
    ///
    /// Same class as the fix in `store_schema` (an unreadable body vetoing the
    /// write that heals it), one layer down: there the *write* was blocked, here
    /// the *boot* is. Paired with that fix, skipping is genuinely recoverable —
    /// the next `store_schema` for the skipped name overwrites the bad row and
    /// the catalog is whole again.
    ///
    /// Skipping is safe here specifically because `schemas` is a **re-derivable**
    /// catalog. It is loud on purpose: every skipped key is logged at WARN with
    /// its decode error, plus a summary count. Do not downgrade these to debug —
    /// a silently short catalog reads as "that schema was never published", and
    /// records under it become unqueryable until the healing write lands.
    pub async fn get_all_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        let scan = self
            .schemas_store
            .scan_items_with_prefix_partition_undecodable::<Schema>("")
            .await?;

        let mut schemas = HashMap::with_capacity(scan.items.len() + scan.undecodable.len());
        for (key, mut schema) in scan.items {
            schema.populate_runtime_fields()?;
            schemas.insert(key, schema);
        }

        let mut skipped = 0usize;
        for (key, error) in &scan.undecodable {
            let opened = match self.schemas_store.inner().get(key.as_bytes()).await? {
                Some(bytes) => decode_catalog_schema(&bytes, self.catalog_unwrap_key.as_ref()).ok(),
                None => None,
            };
            if let Some(mut schema) = opened {
                schema.populate_runtime_fields()?;
                schemas.insert(key.clone(), schema);
                continue;
            }
            skipped += 1;
            tracing::warn!(
                schema = %key,
                error = %error,
                "durable schema row is unreadable; SKIPPING it so the node can boot \
                 (the next write to this schema replaces the bad row). Records under \
                 this schema are unqueryable until then."
            );
        }
        if skipped > 0 {
            tracing::warn!(
                skipped,
                loaded = schemas.len(),
                "schema catalog loaded with unreadable rows skipped"
            );
        }

        Ok(schemas)
    }

    /// Get all schemas for repair through strict, bounded catalog pages.
    ///
    /// The meter repair certifies a complete trust state from this catalog.
    /// The boot reader [`Self::get_all_schemas`] skips unreadable rows so the
    /// node can start; a repair that took that subset would publish
    /// `Reconciled` over schemas it never measured
    /// (papercut-meter-repair-tolerant-catalog-false-complete-20260925).
    /// Keep the two policies separate: boot tolerates, repair refuses.
    pub async fn get_all_schemas_strict(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        let mut cursor = Vec::new();
        let mut rows_by_logical_key: HashMap<String, (String, Schema)> = HashMap::new();
        loop {
            let page = self
                .schemas_store
                .inner()
                .scan_range_paged(&cursor, REPAIR_SCHEMA_SCAN_END, REPAIR_SCHEMA_PAGE_ROWS)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "scan schema catalog for isolated repair: {error}"
                    ))
                })?;
            if page.is_empty() {
                break;
            }
            if page.len() > REPAIR_SCHEMA_PAGE_ROWS
                || page.windows(2).any(|pair| pair[0].0 >= pair[1].0)
                || page.iter().any(|(key, _)| {
                    key.as_slice() < cursor.as_slice() || key.as_slice() >= REPAIR_SCHEMA_SCAN_END
                })
            {
                return Err(SchemaError::InvalidData(
                    "schema catalog repair received an invalid or unordered page".to_string(),
                ));
            }

            for (key_bytes, value_bytes) in &page {
                let key = String::from_utf8(key_bytes.clone()).map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "schema catalog repair found a non-UTF-8 key: {error}"
                    ))
                })?;
                let mut schema =
                    decode_catalog_schema(value_bytes, self.catalog_unwrap_key.as_ref()).map_err(
                        |error| {
                            SchemaError::InvalidData(format!(
                                "schema catalog row {key} is unreadable ({error}); a strict \
                                 catalog read cannot certify the complete catalog"
                            ))
                        },
                    )?;
                if schema.name != key {
                    return Err(SchemaError::InvalidData(format!(
                        "schema catalog repair found unresolved row {key}: payload names {}",
                        schema.name
                    )));
                }
                schema.populate_runtime_fields().map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "schema catalog repair cannot resolve row {key}: {error}"
                    ))
                })?;

                rows_by_logical_key
                    .entry(key.clone())
                    .or_insert((key, schema));
            }

            let last_key = page.last().expect("non-empty page").0.clone();
            if page.len() < REPAIR_SCHEMA_PAGE_ROWS {
                let mut probe_start = last_key.clone();
                probe_start.push(0);
                let trailing = self
                    .schemas_store
                    .inner()
                    .scan_range_paged(&probe_start, REPAIR_SCHEMA_SCAN_END, 1)
                    .await?;
                if !trailing.is_empty() {
                    return Err(SchemaError::InvalidData(
                        "schema catalog repair received a truncated page".to_string(),
                    ));
                }
                break;
            }
            cursor = last_key;
            cursor.push(0);
        }

        Ok(rows_by_logical_key
            .into_values()
            .map(|(_, schema)| (schema.name.clone(), schema))
            .collect())
    }

    /// Get all superseded-by mappings.
    ///
    /// Same skip/unwrap contract as [`Self::get_all_schemas`]: an `ENC:` or
    /// empty row must not fail boot (`SchemaCore::new` loads this map).
    pub async fn get_all_superseded_by(&self) -> Result<HashMap<String, String>, SchemaError> {
        self.load_catalog_map::<String>(&self.superseded_by_store, "superseded_by", &[])
            .await
    }

    /// Get all schema states.
    ///
    /// Boot (`SchemaCore::new`) calls this right after `get_all_schemas`. A
    /// strict scan here re-bricks the node on the same ENC:/empty poison that
    /// #1501 already skips in the schemas collection (probe of #1502 against
    /// Tom's home failed on key `08abb8b3…` with
    /// `expected value at line 1 column 1`).
    pub async fn get_all_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
        // Retention policies and name-claim records intentionally share this
        // node-local namespace, but they are not schema-state rows. Do not
        // mistake their deliberately different JSON shape for a corrupt state
        // row during boot.
        self.load_catalog_map::<SchemaState>(
            &self.schema_states_store,
            "schema_states",
            &[
                NAME_CLAIM_KEY_PREFIX,
                RETENTION_POLICY_KEY_PREFIX,
                RETENTION_ANCHOR_KEY_PREFIX,
            ],
        )
        .await
    }

    async fn load_catalog_map<T: DeserializeOwned + Send + Sync>(
        &self,
        store: &TypedKvStore<dyn KvStore>,
        catalog: &'static str,
        ignored_key_prefixes: &[&str],
    ) -> Result<HashMap<String, T>, SchemaError> {
        let scan = store
            .scan_items_with_prefix_partition_undecodable::<T>("")
            .await?;
        let mut out = HashMap::with_capacity(scan.items.len() + scan.undecodable.len());
        out.extend(scan.items);
        let mut skipped = 0usize;
        for (key, error) in &scan.undecodable {
            if ignored_key_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
            {
                continue;
            }
            let opened = match store.inner().get(key.as_bytes()).await? {
                Some(bytes) => {
                    decode_catalog_json::<T>(&bytes, self.catalog_unwrap_key.as_ref()).ok()
                }
                None => None,
            };
            if let Some(value) = opened {
                out.insert(key.clone(), value);
                continue;
            }
            skipped += 1;
            tracing::warn!(
                catalog,
                key = %key,
                error = %error,
                "durable catalog row is unreadable; SKIPPING it so the node can boot"
            );
        }
        if skipped > 0 {
            tracing::warn!(
                catalog,
                skipped,
                loaded = out.len(),
                "catalog loaded with unreadable rows skipped"
            );
        }
        Ok(out)
    }
}
