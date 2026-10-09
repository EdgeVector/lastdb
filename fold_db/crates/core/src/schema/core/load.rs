use crate::hex::hex_lower;
use std::future::Future;
use std::pin::Pin;

use sha2::{Digest, Sha256};

use super::locks::{lock_map, read_map, write_map};
use super::{CoherenceBinding, SchemaCore};
use crate::schema::types::{Schema, SchemaError};
use crate::schema::SchemaState;
use schema_types::IdentityRecompute;

impl SchemaCore {
    /// Stable compare-and-set token for one schema's physical field map.
    ///
    /// The map is node-local storage metadata, not part of the catalog
    /// identity. Operators use this token to prove that a reviewed repair
    /// still targets the same metadata that the dry run inspected.
    pub fn field_molecule_map_fingerprint(
        map: &std::collections::HashMap<String, String>,
    ) -> String {
        let mut entries: Vec<_> = map.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let mut hash = Sha256::new();
        for (field, molecule) in entries {
            hash.update(field.as_bytes());
            hash.update([0]);
            hash.update(molecule.as_bytes());
            hash.update([0xff]);
        }
        hex_lower(hash.finalize())
    }

    /// Replace one installed schema's physical field map under a compare-and-set guard.
    ///
    /// This changes schema metadata only. It never copies, rewrites, or scans
    /// atom data. The replacement must name every runtime field so a partial
    /// operator input cannot split one schema across two physical maps.
    pub async fn repair_field_molecule_uuids(
        &self,
        schema_name: &str,
        expected_current_fingerprint: &str,
        replacement: std::collections::HashMap<String, String>,
    ) -> Result<Schema, SchemaError> {
        let mut schema = self
            .db_ops
            .get_schema(schema_name)
            .await?
            .ok_or_else(|| SchemaError::NotFound(format!("schema not found: {schema_name}")))?;
        let current = schema.field_molecule_uuids.clone().unwrap_or_default();
        let current_fingerprint = Self::field_molecule_map_fingerprint(&current);
        if current_fingerprint != expected_current_fingerprint {
            return Err(SchemaError::InvalidData(format!(
                "schema molecule map changed: expected {expected_current_fingerprint}, current {current_fingerprint}"
            )));
        }

        if schema.runtime_fields.is_empty() {
            schema.populate_runtime_fields()?;
        }
        let declared: std::collections::HashSet<_> =
            schema.runtime_fields.keys().cloned().collect();
        let proposed: std::collections::HashSet<_> = replacement.keys().cloned().collect();
        if proposed != declared {
            let mut missing: Vec<_> = declared.difference(&proposed).cloned().collect();
            let mut unknown: Vec<_> = proposed.difference(&declared).cloned().collect();
            missing.sort();
            unknown.sort();
            return Err(SchemaError::InvalidData(format!(
                "replacement field map must match every runtime field; missing=[{}], unknown=[{}]",
                missing.join(","),
                unknown.join(",")
            )));
        }

        schema.field_molecule_uuids = Some(replacement);
        schema.runtime_fields.clear();
        schema.populate_runtime_fields()?;
        schema.clear_runtime_molecules();
        self.db_ops.store_schema(schema_name, &schema).await?;
        write_map(&self.schemas, "schemas")?.insert(schema_name.to_string(), schema.clone());
        Ok(schema)
    }

    /// Drop a schema's in-memory cache entry after a failed reload.
    ///
    /// The deferred resident-write persist path calls `store_schema` (durable)
    /// and then `load_schema_internal` (refreshes this cache) as two separate
    /// steps. If the durable store succeeds but the reload fails partway, the
    /// cache keeps whatever it held before the attempt — silently stale
    /// relative to the row that is now actually on disk. Evicting here makes
    /// the next [`Self::get_schema_resolved`] call take its cache-miss branch,
    /// which re-reads the durable row and repopulates the cache from it,
    /// instead of drifting until the next full reload or process restart.
    pub fn evict_stale_schema_cache(&self, schema_name: &str) -> Result<(), SchemaError> {
        write_map(&self.schemas, "schemas")?.remove(schema_name);
        Ok(())
    }

    /// Update an existing schema in both the database and in-memory cache.
    /// Used by ingestion to add Reference topologies after child schemas are resolved.
    pub async fn update_schema(&self, schema: &Schema) -> Result<(), SchemaError> {
        let mut schema = schema.clone();
        if schema.runtime_fields.is_empty() {
            schema.populate_runtime_fields()?;
        }
        schema.clear_runtime_molecules();
        // Gated for the same reason as `load_schema_internal`: an identity
        // minted by a newer algorithm must survive an older binary touching it.
        if let IdentityRecompute::RefusedDowngrade { stored, binary } =
            schema.recompute_identity_hash_unless_newer()
        {
            tracing::warn!(
                schema = %schema.name,
                stored_algo_version = stored,
                binary_algo_version = binary,
                "refusing to downgrade schema identity during update_schema; \
                 keeping the stored identity",
            );
        }

        let name = schema.name.clone();
        self.db_ops.store_schema(&name, &schema).await?;
        write_map(&self.schemas, "schemas")?.insert(name, schema);
        Ok(())
    }

    /// Fetches a schema by name, checking both in-memory cache and database,
    /// **following the `superseded_by` chain** (max 5 hops) when the named
    /// schema is `Blocked` and has an active successor. This is the
    /// resolution the executor uses, so any field/owner-validation gate that
    /// runs alongside the executor must use this method — not
    /// [`Self::get_schema_metadata`] — or it will disagree with what the
    /// executor actually serves (see #618).
    ///
    /// Note: This is STRICTLY case-sensitive.
    pub async fn get_schema_following_supersession(
        &self,
        schema_name: &str,
    ) -> Result<Option<Schema>, SchemaError> {
        self.get_schema_resolved(schema_name, 0, false).await
    }

    /// Read-path variant of [`Self::get_schema_following_supersession`] that
    /// returns a schema whose `runtime_fields` carry NO in-memory molecule (each
    /// is stripped to `None` via [`Schema::clone_for_read`]).
    ///
    /// The query executor takes ownership of the returned schema and re-hydrates
    /// every field from storage before reading it, so handing it the registry's
    /// hydrated molecule was pure waste — an O(field cardinality) deep clone per
    /// query that re-introduced linear scaling on top of the O(1) keyed storage
    /// read (#905). Use this whenever the result drives `resolve_value`; use the
    /// plain variant when the caller needs the full hydrated molecule in memory.
    pub async fn get_schema_following_supersession_for_read(
        &self,
        schema_name: &str,
    ) -> Result<Option<Schema>, SchemaError> {
        self.get_schema_resolved(schema_name, 0, true).await
    }

    /// Internal helper that follows superseded-by chains with a hop counter.
    /// When `for_read` is set, the in-memory hit is returned molecule-stripped
    /// via [`Schema::clone_for_read`] (the hot query path); otherwise the full
    /// hydrated schema is cloned.
    fn get_schema_resolved(
        &self,
        schema_name: &str,
        depth: usize,
        for_read: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Schema>, SchemaError>> + Send + '_>> {
        let schema_name = schema_name.to_string();
        Box::pin(async move {
            let schema_name = schema_name.as_str();
            const MAX_REDIRECT_HOPS: usize = 5;

            if depth > MAX_REDIRECT_HOPS {
                return Err(SchemaError::InvalidData(format!(
                    "Superseded-by chain for schema '{schema_name}' exceeds maximum depth of {MAX_REDIRECT_HOPS}"
                )));
            }

            // Check if this schema is blocked and has a successor
            let successor = {
                let state = lock_map(&self.schema_states, "schema_states")?
                    .get(schema_name)
                    .copied();
                if state == Some(SchemaState::Blocked) {
                    lock_map(&self.superseded_by, "superseded_by")?
                        .get(schema_name)
                        .cloned()
                } else {
                    None
                }
            };

            if let Some(new_name) = successor {
                tracing::info!(
                    "Schema '{}' is blocked with successor, redirecting to '{}'",
                    schema_name,
                    new_name
                );
                return self
                    .get_schema_resolved(&new_name, depth + 1, for_read)
                    .await;
            }

            // 1. Try exact match in memory
            if for_read {
                // Hot query path: clone the cached schema's structure WITHOUT
                // deep-copying its hydrated molecules (the executor re-hydrates
                // from storage anyway). Uses the `&self` `clone_for_read_shared`
                // so this runs under a SHARED read lock — N concurrent readers
                // resolve their schema in parallel instead of serializing
                // through one exclusive lock (the convoy this card fixed).
                let stripped = {
                    let schemas = read_map(&self.schemas, "schemas")?;
                    schemas.get(schema_name).map(Schema::clone_for_read_shared)
                };
                if let Some(schema) = stripped {
                    return Ok(Some(schema));
                }
            } else if let Some(schema) = self.get_schema_metadata(schema_name)? {
                return Ok(Some(schema));
            }

            // 2. Try exact match in database (refresh cache if found)
            if let Some(schema) = self
                .db_ops
                .get_schema(schema_name)
                .await
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?
            {
                // Update memory
                let cached_name = schema.name.clone();
                self.load_schema_internal(schema.clone()).await?;
                tracing::info!(
                    "Refreshed schema '{}' from database (stale cache)",
                    cached_name
                );
                if for_read {
                    // Return the molecule-stripped cached copy for consistency
                    // with the in-memory hot path. `load_schema_internal`
                    // populated runtime_fields on the cache entry. Shared read
                    // lock + `&self` clone, same as the in-memory hot path above.
                    let stripped = {
                        let schemas = read_map(&self.schemas, "schemas")?;
                        schemas.get(&cached_name).map(Schema::clone_for_read_shared)
                    };
                    if let Some(stripped) = stripped {
                        return Ok(Some(stripped));
                    }
                }
                return Ok(Some(schema));
            }

            Ok(None)
        }) // end Box::pin
    }

    /// Adopt the LOCAL predecessor's effective field→molecule map when loading
    /// an externally-expanded schema, so the expanded identity reads and writes
    /// the SAME molecules the predecessor's rows live in.
    ///
    /// Why derivation is not enough: a field's *effective* molecule is
    /// write-history-dependent — `ensure_molecule` re-anchors a mapped field to
    /// `deterministic(own_name, field)` when the mapped molecule does not exist
    /// on disk at first write — so the predecessor's persisted
    /// `field_molecule_uuids` (its ground truth, synced across its own loads
    /// and writes) can disagree per-field with what mapper derivation computes.
    /// A schema-service expansion re-derives from mappers
    /// (`expand_schema` clears `field_molecule_uuids`), so loading its product
    /// pointed enumeration at empty molecules and every historical row
    /// vanished under the expanded identity (lastgit LastgitCrEvent expansion
    /// on the CiStatus-shared collection; card
    /// fold-schema-expansion-shared-collection-rows-invisible). The node-side
    /// approval path solves this with `apply_field_mappers`; this is the
    /// missing equivalent for the load path, keyed on the predecessor
    /// relationship rather than the (service-side, possibly stale-anchored)
    /// mapper graph.
    ///
    /// A predecessor is a DIFFERENT local identity with the same normalized
    /// `descriptive_name` (the service decorates expansions with a trailing
    /// parenthesized token), the same owner, the same schema_type and key
    /// layout, whose field set is a SUBSET of the incoming schema's — i.e.
    /// exactly the schema this one expands. The subset gate is what makes the
    /// relation asymmetric: loading the OLD schema never adopts from the NEW.
    /// Fields the predecessor has no entry for keep their derived molecule
    /// (fresh fields get fresh molecules on first write, as before).
    fn adopt_predecessor_molecule_uuids(
        &self,
        schema: &mut Schema,
        own_stored: Option<&Schema>,
    ) -> Result<(), SchemaError> {
        fn descriptive_base(s: &str) -> &str {
            let t = s.trim_end();
            if t.ends_with(')') {
                if let Some(open) = t.rfind(" (") {
                    return &t[..open];
                }
            }
            t
        }
        fn norm_owner(s: Option<&str>) -> Option<&str> {
            s.filter(|x| !x.is_empty())
        }
        fn key_fingerprint(s: &Schema) -> (Option<String>, Option<String>) {
            let norm = |v: Option<&str>| {
                v.map(str::trim)
                    .filter(|x| !x.is_empty())
                    .map(str::to_string)
            };
            match s.key.as_ref() {
                Some(k) => (
                    norm(k.hash_field.as_deref()),
                    norm(k.range_field.as_deref()),
                ),
                None => (None, None),
            }
        }

        // Same-identity continuity first: a stored record for THIS identity is
        // the node's local truth (it carries previously adopted or
        // write-established molecules). Restoring it here — BEFORE the first
        // sync — makes reloads order-independent: the wire copy's
        // service-derived values never overwrite what this node's data
        // actually lives under, and boot order (expansion before predecessor)
        // stops mattering once the first load persisted the adopted map.
        //
        // EXCEPT when the incoming copy carries a DIFFERENT (non-empty) mapper
        // set than the stored record: a catalog refresh that delivers new
        // mappers is a deliberate re-point, and derivation from those mappers
        // must win (contract:
        // same_name_catalog_reload_replaces_mapperless_definition_durably).
        let field_mapper_refresh = {
            let incoming = schema.field_mappers.as_ref().filter(|m| !m.is_empty());
            let stored = own_stored
                .and_then(|st| st.field_mappers.as_ref())
                .filter(|m| !m.is_empty());
            match incoming {
                Some(incoming) => stored != Some(incoming),
                None => false,
            }
        };
        let record_mapper_refresh = match (
            schema.record_mapper.as_ref(),
            own_stored.and_then(|st| st.record_mapper.as_ref()),
        ) {
            (Some(incoming), stored) => stored != Some(incoming),
            (None, _) => false,
        };
        let mapper_refresh = field_mapper_refresh || record_mapper_refresh;
        if !mapper_refresh {
            if let Some(stored_r) = own_stored.and_then(|st| st.molecule_uuid.clone()) {
                if !stored_r.is_empty() {
                    schema.molecule_uuid = Some(stored_r);
                }
            }
        }
        if let Some(stored_map) = own_stored
            .filter(|_| !mapper_refresh)
            .and_then(|st| st.field_molecule_uuids.as_ref())
            .filter(|m| !m.is_empty())
        {
            let mut restored = 0usize;
            for (field_name, field) in &mut schema.runtime_fields {
                if let Some(uuid) = stored_map.get(field_name) {
                    if field.common().molecule_uuid() != Some(uuid) {
                        field.common_mut().set_molecule_uuid(uuid.clone());
                        restored += 1;
                    }
                }
            }
            if restored > 0 {
                tracing::info!(
                    schema = %schema.name,
                    restored,
                    "restored this identity's stored molecule map over wire-derived values"
                );
            }
            return Ok(());
        }

        let Some(desc) = schema.descriptive_name.clone() else {
            return Ok(());
        };
        let base = descriptive_base(&desc).to_string();
        let incoming_fields: std::collections::HashSet<String> = schema
            .fields
            .as_ref()
            .map(|f| f.iter().cloned().collect())
            .unwrap_or_default();
        if incoming_fields.is_empty() {
            return Ok(());
        }
        let incoming_key = key_fingerprint(schema);

        let predecessor = {
            let schemas = read_map(&self.schemas, "schemas")?;
            schemas
                .values()
                .filter(|c| c.name != schema.name)
                .filter(|c| {
                    norm_owner(c.owner_app_id.as_deref())
                        == norm_owner(schema.owner_app_id.as_deref())
                })
                .filter(|c| {
                    c.descriptive_name
                        .as_deref()
                        .is_some_and(|d| descriptive_base(d) == base)
                })
                .filter(|c| c.schema_type == schema.schema_type)
                .filter(|c| key_fingerprint(c) == incoming_key)
                .filter(|c| {
                    c.fields.as_ref().is_some_and(|fs| {
                        !fs.is_empty() && fs.iter().all(|f| incoming_fields.contains(f))
                    })
                })
                .filter(|c| {
                    c.field_molecule_uuids
                        .as_ref()
                        .is_some_and(|m| !m.is_empty())
                        || c.molecule_uuid.as_ref().is_some_and(|u| !u.is_empty())
                })
                .max_by_key(|c| c.fields.as_ref().map_or(0, Vec::len))
                .map(|c| {
                    (
                        c.name.clone(),
                        c.field_molecule_uuids.clone().unwrap_or_default(),
                        c.molecule_uuid.clone(),
                    )
                })
        };
        let Some((pred_name, pred_uuids, pred_r)) = predecessor else {
            return Ok(());
        };

        let mut adopted = 0usize;
        for (field_name, field) in &mut schema.runtime_fields {
            if let Some(uuid) = pred_uuids.get(field_name) {
                if field.common().molecule_uuid() != Some(uuid) {
                    field.common_mut().set_molecule_uuid(uuid.clone());
                    adopted += 1;
                }
            }
        }
        if let Some(r) = pred_r.filter(|u| !u.is_empty()) {
            if schema.molecule_uuid.as_deref() != Some(r.as_str()) {
                schema.molecule_uuid = Some(r);
                adopted += 1;
            }
        }
        if adopted > 0 {
            tracing::info!(
                schema = %schema.name,
                predecessor = %pred_name,
                adopted,
                "adopted predecessor molecule map for shared fields (schema expansion \
                 data continuity — reads/writes stay on the predecessor's molecules)"
            );
        }
        Ok(())
    }

    pub async fn load_schema_internal(&self, schema: Schema) -> Result<(), SchemaError> {
        // Ensure runtime_fields are populated. Schemas arriving from the schema
        // service have runtime_fields empty (it's #[serde(skip)]). Without this
        // call, mutations fail with "Field not found in runtime_fields".
        // Only populate if empty — callers like interpret_declarative_schema may
        // have already populated and set additional state (molecule UUIDs, policies).
        let mut schema = schema;
        if schema.runtime_fields.is_empty() {
            schema.populate_runtime_fields()?;
        }
        // Mint any field identity the catalog did not supply. Schema Service
        // mints these when it is in the loop, but most schemas on a local node
        // arrive without them, and a node that cannot name its own fields can
        // never detect that two of its schemas index the same record. Fills only
        // what is missing, so a Schema-Service-minted identity always wins, and
        // uses the same `compute_field_hash` formula so both agree.
        schema.ensure_field_hashes();
        // Data continuity for an externally-expanded schema: restore this
        // identity's own stored molecule map, or adopt the LOCAL predecessor's,
        // BEFORE the first sync persists derived values
        // (see `adopt_predecessor_molecule_uuids`). The stored row is fetched
        // once here and reused by the existing-schema branch below.
        let existing_schema = self.db_ops.get_schema(&schema.name).await?;
        self.adopt_predecessor_molecule_uuids(&mut schema, existing_schema.as_ref())?;
        // Persist field → molecule UUID map for field_hash protein bind and
        // multi-key coherence (runtime molecules themselves are still cleared).
        schema.sync_molecule_uuids();

        // Drop any materialized in-memory molecules before this schema enters
        // the in-memory cache. The cache is *cloned* on every query
        // (`get_schema_following_supersession`), so a schema carrying
        // fully-materialized molecules makes each clone — and its eventual
        // drop — O(field cardinality). That cost (cloning/dropping every key on
        // every keyed read) is exactly what swamped the per-key O(1) read
        // primitive and kept `indexed_point_lookup` scaling O(field) even
        // though the lookup itself touches a single key. The molecule data is
        // redundant here: the `mk:`/`mh:` per-key records on disk are the
        // source of truth, and both the read path (`refresh_for_read` /
        // `refresh_from_db`) and the write path (`restore_missing_molecules`)
        // re-hydrate from each field's retained `molecule_uuid` on demand. So
        // the cache only needs the field metadata, never the data.
        schema.clear_runtime_molecules();

        // TH6a — cache the schema's identity hash (`get_identity_hash`) so
        // the firing-snapshot writer can stamp `schema_versions` on every
        // capture without a per-fire recompute. Recomputed on load because the
        // input may have come from the schema service (no hash set) or from a
        // re-registration with changed fields.
        //
        // The recompute is GATED. It used to be unconditional, which meant a
        // binary implementing an older algorithm overwrote a correct hash with
        // its own answer and left no trace — indistinguishable from an upgrade.
        // That is exactly how 43 rows on the primary ended up stale. An
        // identity stamped with a NEWER `algo_version` than this binary
        // implements is now preserved and reported.
        if let IdentityRecompute::RefusedDowngrade { stored, binary } =
            schema.recompute_identity_hash_unless_newer()
        {
            tracing::warn!(
                schema = %schema.name,
                stored_algo_version = stored,
                binary_algo_version = binary,
                identity_hash = %schema.identity_hash.as_deref().unwrap_or("<none>"),
                "refusing to downgrade schema identity: this schema was minted by a \
                 newer identity algorithm than this binary implements. Keeping the \
                 stored identity. This node is older than its own data — upgrade it \
                 rather than re-registering the schema.",
            );
        }

        let name = schema.name.clone();

        // `existing_schema` was fetched before the adoption step above.

        // Second downgrade guard, for the case the first cannot see: the
        // incoming definition carries no identity (or a legacy one) but the row
        // already on disk was stamped by a newer binary. Recomputing above is
        // harmless; *persisting* it over the stored row would be the downgrade.
        // Adopt the stored identity instead.
        if let Some(stored_row) = existing_schema.as_ref() {
            if let Some(stored) = schema_types::refuses_identity_downgrade(
                stored_row.identity_hash.as_deref(),
                stored_row.identity_hash_algo_version,
            ) {
                tracing::warn!(
                    schema = %name,
                    stored_algo_version = stored,
                    binary_algo_version = schema_types::IDENTITY_HASH_ALGO_VERSION,
                    "refusing to overwrite a stored schema identity minted by a newer \
                     identity algorithm than this binary implements. Keeping the stored \
                     identity for this row.",
                );
                schema.identity_hash = stored_row.identity_hash.clone();
                schema.identity_hash_algo_version = stored_row.identity_hash_algo_version;
            }
        }

        if let Some(existing_schema) = existing_schema {
            // A same-hash load is a metadata refresh, not a no-op. This matters
            // when a local/older definition was cached before the catalog
            // identity gained field_mappers: keeping the mapper-less copy makes
            // an expansion look empty even though the identity hash matches.
            //
            // Preserve molecule UUIDs for fields owned by this schema, but let
            // incoming catalog mappers win for mapped fields. Otherwise a UUID
            // created by a premature local mint would override the mapper and
            // keep reads pointed at the wrong molecule.
            let mapped_fields: std::collections::HashSet<String> = schema
                .field_mappers
                .as_ref()
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            let mut merged_molecules = schema.field_molecule_uuids.take().unwrap_or_default();
            if let Some(existing_molecules) = existing_schema.field_molecule_uuids {
                for (field, molecule_uuid) in existing_molecules {
                    if !mapped_fields.contains(&field) {
                        merged_molecules.entry(field).or_insert(molecule_uuid);
                    }
                }
            }
            schema.field_molecule_uuids =
                (!merged_molecules.is_empty()).then_some(merged_molecules);
            schema.populate_runtime_fields()?;
            schema.sync_molecule_uuids();
            schema.clear_runtime_molecules();

            // Persist the refreshed catalog metadata as well as caching it so
            // mapper repair survives a daemon restart.
            self.db_ops.store_schema(&name, &schema).await?;
            write_map(&self.schemas, "schemas")?.insert(name.clone(), schema);

            // Preserve existing state from database
            // `get_schema_state` reports an unreadable durable row as missing.
            // Keep the state boot already cached for this schema in that case,
            // so a reload cannot silently turn a Blocked schema Available.
            let existing_state = self.db_ops.get_schema_state(&name).await?;
            let state = match existing_state {
                Some(state) => state,
                None => lock_map(&self.schema_states, "schema_states")?
                    .get(&name)
                    .copied()
                    .unwrap_or(SchemaState::Available),
            };
            lock_map(&self.schema_states, "schema_states")?.insert(name.clone(), state);
        } else {
            // New schema - persist to database and update in-memory caches
            self.db_ops.store_schema(&name, &schema).await?;
            self.db_ops
                .store_schema_state(&name, &SchemaState::Available)
                .await?;

            write_map(&self.schemas, "schemas")?.insert(name.clone(), schema);
            lock_map(&self.schema_states, "schema_states")?
                .insert(name.clone(), SchemaState::Available);
        }

        // Multi-key coherence: when this schema is another key layout over a
        // product already loaded, bind their matching fields into proteins so
        // writes fold across both partitions. Non-blocking.
        self.apply_field_hash_coherence_on_load(&name).await?;

        Ok(())
    }

    /// Bind this schema's fields to the proteins of any multi-key sibling.
    ///
    /// Detection is entirely local: [`are_multi_key_siblings`] over the field
    /// identities minted in [`Self::load_schema_internal`]. Identity equality on
    /// its own is deliberately not enough to bind — see the module docs on
    /// `field_hash_coherence` for the `created_at` case that makes it unsafe.
    ///
    /// Peers are scanned under a shared read lock over the schema cache, reading
    /// only each candidate's key, owner, and field identities. Nothing is cloned
    /// until a sibling is actually found, so the pass stays cheap on a catalog of
    /// a thousand-plus schemas.
    pub async fn apply_field_hash_coherence_on_load(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        use crate::atom::deterministic_molecule_uuid;
        use crate::schema::field_hash_coherence::{
            are_multi_key_siblings, bind_cross_key_field_protein, shared_field_identities,
            KeyLayoutFingerprint,
        };

        let Some(loaded) = self.get_schema_metadata(schema_name)? else {
            return Ok(());
        };
        if loaded.field_hashes.is_empty() {
            return Ok(());
        }
        let target_layout = KeyLayoutFingerprint::from_schema(&loaded);
        if !target_layout.is_keyable() {
            return Ok(());
        }

        // (own field, field_hash, peer molecule, peer layout) for every sibling
        // match. The peer's local field name may differ from ours — matching is
        // on identity, not on the name — so each side's molecule is looked up
        // under its OWN name. Using one name for both is what would break the
        // rename case.
        //
        // This scan is in-memory and cheap even at a thousand-plus schemas
        // (measured: 3.8ms for a no-sibling schema against a 1166-schema
        // catalog on the primary, 2026-08-17). It is the DURABLE step below
        // that must not repeat, so the scan stays unconditional and its result
        // is what the memo compares.
        let bindings: Vec<CoherenceBinding> = {
            let schemas = read_map(&self.schemas, "schemas")?;
            let mut out = Vec::new();
            for (peer_name, peer) in schemas.iter() {
                if peer_name == schema_name || !are_multi_key_siblings(peer, &loaded) {
                    continue;
                }
                let peer_layout = KeyLayoutFingerprint::from_schema(peer);
                // `peer` is the `a` side, `loaded` the `b` side.
                for shared in shared_field_identities(peer, &loaded) {
                    let peer_mol = peer
                        .field_molecule_uuids
                        .as_ref()
                        .and_then(|m| m.get(&shared.a_field))
                        .cloned()
                        .unwrap_or_else(|| deterministic_molecule_uuid(peer_name, &shared.a_field));
                    out.push(CoherenceBinding {
                        field: shared.b_field,
                        field_hash: shared.field_hash,
                        peer_molecule: peer_mol,
                        peer_layout: peer_layout.clone(),
                    });
                }
            }
            // Peer iteration is over a HashMap, so the order varies per call.
            // Sort before comparing, or an unchanged binding set would compare
            // unequal at random and the memo would never hit.
            out.sort_by(|x, y| {
                (&x.field_hash, &x.peer_molecule, &x.field).cmp(&(
                    &y.field_hash,
                    &y.peer_molecule,
                    &y.field,
                ))
            });
            out
        };
        if bindings.is_empty() {
            return Ok(());
        }

        // Already established durably by this process, and nothing about the
        // binding set has changed — the durable half below would re-derive the
        // same proteins and rewrite the same breadcrumbs. See the
        // `coherence_bound` field docs for why this is safe when a new sibling
        // schema is registered later.
        if lock_map(&self.coherence_bound, "coherence_bound")?
            .get(schema_name)
            .is_some_and(|prev| prev == &bindings)
        {
            return Ok(());
        }

        let store = self.db_ops.atoms();
        let mut bound = 0usize;
        let mut failed = 0usize;
        for binding in &bindings {
            let own_mol = loaded
                .field_molecule_uuids
                .as_ref()
                .and_then(|m| m.get(&binding.field))
                .cloned()
                .unwrap_or_else(|| deterministic_molecule_uuid(schema_name, &binding.field));
            // Peer first: it is the established member and may already own tips.
            match bind_cross_key_field_protein(
                store,
                &binding.field_hash,
                &binding.peer_molecule,
                &binding.peer_layout,
                &own_mol,
                &target_layout,
            )
            .await
            {
                Ok(Some(_)) => bound += 1,
                Ok(None) => {}
                Err(e) => {
                    failed += 1;
                    tracing::warn!(
                        target: "field_hash_coherence",
                        schema = %schema_name,
                        field = %binding.field,
                        error = %e,
                        "auto protein bind on schema load failed (non-fatal)"
                    );
                }
            }
        }
        if bound > 0 {
            tracing::info!(
                target: "field_hash_coherence",
                schema = %schema_name,
                fields = bound,
                "bound multi-key sibling fields into proteins"
            );
        }

        // Memoize only a clean sweep. A binding that errored is not established,
        // and recording it would turn one transient store failure into a
        // permanently unbound field for the life of the process — the pass would
        // never look at it again.
        if failed == 0 {
            lock_map(&self.coherence_bound, "coherence_bound")?
                .insert(schema_name.to_string(), bindings);
        }

        Ok(())
    }
}
