//! Schema domain store.
//!
//! Owns the storage namespaces for schemas, schema states, and
//! schema supersede-by mappings. External callers access schema
//! operations through this type via `DbOperations::schemas()`.

use crate::crypto::at_rest::is_sealed_at_rest;
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
}

impl SchemaStore {
    pub(crate) fn new(
        schemas_store: Arc<TypedKvStore<dyn KvStore>>,
        schema_states_store: Arc<TypedKvStore<dyn KvStore>>,
        superseded_by_store: Arc<TypedKvStore<dyn KvStore>>,
    ) -> Self {
        Self {
            schemas_store,
            schema_states_store,
            superseded_by_store,
            catalog_unwrap_key: None,
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

    /// Get a specific schema by name.
    ///
    /// Query falls back here on a cache miss (`get_schema_following_supersession_for_read`).
    /// A serde failure on an `ENC:` tip used to become HTTP 400
    /// `expected value at line 1 column 1` and blind `kanban list`. Open the
    /// envelope when we have the content key; otherwise treat the row as
    /// missing (same as boot-skip) — never return the serde error.
    pub async fn get_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        let Some(bytes) = self
            .schemas_store
            .inner()
            .get(schema_name.as_bytes())
            .await?
        else {
            return Ok(None);
        };

        let was_sealed = is_sealed_at_rest(&bytes);
        let mut schema = match decode_catalog_schema(&bytes, self.catalog_unwrap_key.as_ref()) {
            Ok(schema) => schema,
            Err(e) => {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "durable schema row is unreadable; treating as missing so \
                     query does not 400 (boot-skip already left it out of cache)"
                );
                return Ok(None);
            }
        };
        schema.populate_runtime_fields()?;

        if was_sealed {
            // Heal the tip so the next boot/get is JSON. Failure is non-fatal:
            // we already have a usable schema for this request.
            if let Err(e) = self.schemas_store.put_item(schema_name, &schema).await {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "opened ENC: catalog tip but could not persist the JSON heal"
                );
            } else if let Err(e) = self.schemas_store.inner().flush().await {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "opened ENC: catalog tip; JSON heal flush failed"
                );
            }
        }

        Ok(Some(schema))
    }

    /// Get the state of a specific schema.
    ///
    /// Same contract as [`Self::get_schema`] and the boot scan
    /// ([`Self::get_all_schema_states`]): an `ENC:` tip opens with the catalog
    /// unwrap key and heals to JSON; a row that still does not decode is
    /// treated as missing and logged, never returned as a serde error.
    ///
    /// Every hard-erasure finalize reloads its schema through
    /// `load_schema_internal`, which reads this row. A strict read here made
    /// every Delete on a schema with a sealed state tip fail with
    /// `Serialization error: expected value at line 1 column 1` after its
    /// destructive step, on every retry, until the persist lane quarantined it
    /// (both BoardCards_hashrange_v1 schemas on the primary, 2026-09-24; their
    /// state tips were written `ENC:` by the 2026-08-16 mis-wired sealer).
    pub async fn get_schema_state(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaState>, SchemaError> {
        let mut found = None;
        for form in crate::kind_partition::read_forms(schema_name) {
            if let Some(bytes) = self
                .schema_states_store
                .inner()
                .get(form.as_bytes())
                .await?
            {
                found = Some(bytes);
                break;
            }
        }
        let Some(bytes) = found else {
            return Ok(None);
        };
        let was_sealed = is_sealed_at_rest(&bytes);
        let state =
            match decode_catalog_json::<SchemaState>(&bytes, self.catalog_unwrap_key.as_ref()) {
                Ok(state) => state,
                Err(e) => {
                    tracing::warn!(
                        schema = %schema_name,
                        error = %e,
                        "durable schema state row is unreadable; treating it as missing \
                         (boot-skip already left it out of the state cache)"
                    );
                    return Ok(None);
                }
            };
        if was_sealed {
            // Heal so the next read is plain JSON. Non-fatal: this request
            // already has a usable state.
            if let Err(e) = self.schema_states_store.put_item(schema_name, &state).await {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "opened ENC: schema state tip but could not persist the JSON heal"
                );
            } else if let Err(e) = self.schema_states_store.inner().flush().await {
                tracing::warn!(
                    schema = %schema_name,
                    error = %e,
                    "opened ENC: schema state tip; JSON heal flush failed"
                );
            }
        }
        Ok(Some(state))
    }

    async fn require_installed_schema(&self, schema_name: &str) -> Result<(), SchemaError> {
        if self.get_schema(schema_name).await?.is_some() {
            Ok(())
        } else {
            Err(SchemaError::NotFound(schema_name.to_string()))
        }
    }

    /// Store a schema.
    ///
    /// **Skip-if-unchanged:** when the durable catalog body already matches
    /// `schema` (JSON-equality of the serialized `Schema`, which skips
    /// runtime-only fields), this is a no-op. Product mutations used to call
    /// this on every write; without the skip, LastStore append-only segments
    /// retained every full catalog rewrite and the `schemas` plane ballooned
    /// (~354× historical copies measured on a daily-driver home).
    ///
    /// **Won't-undo:** the skip read must never be able to fail the write. An
    /// undeserializable durable body (empty row, legacy `ENC:` value read
    /// through a plain seam, truncated segment) used to propagate out of the
    /// `get_item` and abort `store_schema` entirely, so the one write that
    /// would have replaced the bad row was the one write that could not run.
    /// On Tom's primary that latched two kanban schemas into permanent
    /// `Serialization error: expected value at line 1 column 1` failures,
    /// which blinded the BoardCards projection and stalled the whole factory.
    /// The skip is an optimization; treat an unreadable existing body as
    /// "not equal" and fall through to the put that heals it.
    pub async fn store_schema(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        match self.schemas_store.get_item::<Schema>(schema_name).await {
            Ok(Some(existing)) if durable_schema_eq(&existing, schema) => return Ok(()),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    schema = %schema_name,
                    "durable schema body unreadable; overwriting it instead of \
                     failing the write (skip-if-unchanged is an optimization)"
                );
            }
        }
        self.schemas_store.put_item(schema_name, schema).await?;
        self.schemas_store.inner().flush().await?;
        Ok(())
    }

    /// Store schema state.
    ///
    /// Same skip-if-unchanged contract as [`Self::store_schema`], including the
    /// won't-undo: an unreadable durable body must not veto the write that
    /// replaces it.
    pub async fn store_schema_state(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        match self
            .schema_states_store
            .get_item::<SchemaState>(schema_name)
            .await
        {
            Ok(Some(existing)) if existing == *state => return Ok(()),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    schema = %schema_name,
                    "durable schema state unreadable; overwriting it instead of \
                     failing the write (skip-if-unchanged is an optimization)"
                );
            }
        }
        self.schema_states_store
            .put_item(schema_name, state)
            .await?;
        self.schema_states_store.inner().flush().await?;
        Ok(())
    }

    /// Remove one installed schema identity from the catalog.
    ///
    /// Point Deletes of the `schemas` row, the `schema_states` row, the
    /// name-claim row, and the retention keys for this identity. Product
    /// tips and atoms are left for a later janitor. Returns whether a
    /// catalog body was present before the Deletes. A miss still runs the
    /// Deletes so a retry is idempotent.
    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        let existing = self.get_schema(schema_name).await?;
        let existed = existing.is_some();
        if let Some(schema) = existing {
            let mut field_molecule_uuids: Vec<String> = schema
                .field_molecule_uuids
                .unwrap_or_default()
                .into_values()
                .collect();
            field_molecule_uuids.sort();
            field_molecule_uuids.dedup();
            let receipt = SchemaDropReceipt {
                identity: schema_name.to_string(),
                owner_app: schema.owner_app_id.clone(),
                field_molecule_uuids,
                dropped_at_unix_ms: crate::clock::unix_millis(),
            };
            self.schema_states_store
                .put_item(&schema_drop_receipt_key(schema_name), &receipt)
                .await?;
        }
        let mut state_keys = vec![
            schema_name.to_string(),
            name_claim_key(schema_name),
            retention_policy_key(schema_name),
        ];
        state_keys.extend(
            self.schema_states_store
                .list_keys_with_prefix(&retention_age_prefix(schema_name))
                .await?,
        );
        state_keys.extend(
            self.schema_states_store
                .list_keys_with_prefix(&retention_age_latest_prefix(schema_name))
                .await?,
        );
        let _ = self.schemas_store.delete_item(schema_name).await?;
        let _ = self.superseded_by_store.delete_item(schema_name).await?;
        self.schema_states_store
            .batch_delete_keys(state_keys)
            .await?;
        self.flush().await?;
        Ok(existed)
    }

    /// Point-get the janitor receipt for one dropped identity.
    pub async fn get_schema_drop_receipt(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaDropReceipt>, SchemaError> {
        Ok(self
            .schema_states_store
            .get_item(&schema_drop_receipt_key(schema_name))
            .await?)
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
                let logical_key = crate::kind_partition::logical_row_id(&key);
                if crate::kind_partition::logical_row_id(&schema.name) != logical_key {
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

                // Ordered storage places the anchored NUL form before its
                // legacy colon twin, so the first logical row is authoritative.
                rows_by_logical_key
                    .entry(logical_key)
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

    /// Store a schema superseded-by mapping (old_name → new_name)
    pub async fn store_superseded_by(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), SchemaError> {
        self.superseded_by_store
            .put_item(old_name, &new_name.to_string())
            .await?;
        self.superseded_by_store.inner().flush().await?;
        Ok(())
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
            &[NAME_CLAIM_KEY_PREFIX, RETENTION_POLICY_KEY_PREFIX],
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
