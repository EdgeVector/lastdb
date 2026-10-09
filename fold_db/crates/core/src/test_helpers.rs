//! Test utilities for building schemas with proper classifications.
//! Common helpers remain available to integration tests and downstream crates.

/// A node whose molecule keys are really blinded/order-preserving — the
/// configuration a real home runs and almost no test builds. Only compiled for
/// tests: it opens a `tempfile` home and generates a signer.
#[cfg(any(test, feature = "test-utils"))]
pub mod encrypted_home;

use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

static PURGE_WALK_STALL: Mutex<Option<Arc<tokio::sync::Notify>>> = Mutex::new(None);

/// Open a fixture with one identity retained across all of its reopens.
/// This preserves the normal local backend, capture wrapper, and author clock.
pub async fn open_with_signer(
    path: &str,
    signer: Arc<crate::security::Ed25519KeyPair>,
) -> Result<crate::fold_db_core::FoldDB, crate::storage::StorageError> {
    crate::fold_db_core::FoldDB::new_with_test_signer(path, signer).await
}

/// Read one fixture's durable tip by its exact physical key. This does not
/// hydrate the resident graph, enumerate a molecule, or expose a raw store.
pub async fn persisted_tip_entry(
    db: &crate::fold_db_core::FoldDB,
    molecule_uuid: &str,
    hash: &str,
    range: &str,
) -> Option<crate::atom::AtomEntry> {
    db.db_ops()
        .atoms()
        .raw()
        .get_item::<crate::db_operations::atom_store::PerKeyRecord>(
            &crate::atom::molecule_key_codec::hash_range_record_key(molecule_uuid, hash, range),
        )
        .await
        .expect("fixture durable tip read")
        .map(|record| record.entry)
}

/// Durable tip for an API `(hash, range)`. Plain homes hit the physical key
/// directly. BlindV1/OpeV1 homes encode first, then read the storage slot.
pub async fn persisted_tip_entry_api(
    db: &crate::fold_db_core::FoldDB,
    molecule_uuid: &str,
    api_hash: &str,
    api_range: &str,
) -> Option<crate::atom::AtomEntry> {
    if let Some(entry) = persisted_tip_entry(db, molecule_uuid, api_hash, api_range).await {
        return Some(entry);
    }
    let atoms = db.db_ops().atoms();
    let storage_hash = atoms.storage_hash(molecule_uuid, api_hash).ok()?;
    let storage_range = atoms.storage_range(molecule_uuid, api_range).ok()?;
    persisted_tip_entry(db, molecule_uuid, &storage_hash, &storage_range).await
}

/// Run the real atom-GC path at an explicit fixture clock cut. Tests first
/// drain their writes and read the stored atom timestamps. This keeps byte
/// reclaim proofs independent of a host wall-clock adjustment while retaining
/// the production recent-atom guard and pin-log roots.
pub async fn gc_atoms_at_fixture_cut(
    db: &crate::fold_db_core::FoldDB,
    dry_run: bool,
    cut: chrono::DateTime<chrono::Utc>,
) -> crate::db_operations::admin_db::AtomGcReport {
    let roots = db
        .mutation_manager()
        .pending_pin_log_atom_roots()
        .await
        .expect("fixture pin-log roots");
    db.db_ops()
        .atoms()
        .gc_orphan_atoms_with_roots_at_cut(dry_run, None, false, &roots, cut)
        .await
        .expect("fixture GC at explicit clock cut")
}

/// Stall the guarded-complement retain walk until the returned notify is
/// signalled. Integration tests use this to prove a concurrent write still
/// completes while a one-record purge is walking.
pub fn set_purge_walk_stall(notify: Option<Arc<tokio::sync::Notify>>) {
    *PURGE_WALK_STALL.lock().expect("purge walk stall") = notify;
}

pub(crate) async fn wait_purge_walk_stall() {
    let notify = PURGE_WALK_STALL.lock().expect("purge walk stall").clone();
    if let Some(notify) = notify {
        notify.notified().await;
    }
}

/// Hold the exclusive schema purge barrier. A concurrent write must still
/// complete: writers do not take this lock, and the persist lane must drain
/// while a purge holds it.
pub async fn hold_schema_purge_barrier(
    db: &crate::fold_db_core::FoldDB,
    schema: &str,
) -> tokio::sync::OwnedRwLockWriteGuard<()> {
    db.mutation_manager()
        .schema_purge_barrier(schema)
        .write_owned()
        .await
}

/// Bring a throwaway test store's tip-version reverse-reference plane to a
/// complete state before exercising purge. Production nodes do this through
/// the background reindex; direct `FoldDB` fixtures do not run that driver.
pub async fn complete_tip_version_backrefs(db: &crate::fold_db_core::FoldDB) {
    loop {
        let report = db
            .db_ops()
            .atoms()
            .reindex_tip_version_backrefs(None, Some(256))
            .await
            .expect("complete tip-version backrefs for test fixture");
        if report.completed {
            break;
        }
    }
}

/// Bring a throwaway store's full atom reverse-edge plane to replay-complete.
pub async fn complete_atom_ref_edges(db: &crate::fold_db_core::FoldDB) {
    loop {
        let report = db
            .db_ops()
            .atoms()
            .reindex_atom_ref_v2_edges(None, Some(256))
            .await
            .expect("complete compact atom reverse edges for test fixture");
        if report.completed {
            break;
        }
    }
    db.db_ops()
        .atoms()
        .mark_atom_ref_v2_history_complete(None)
        .await
        .expect("mark compact atom reverse-edge history complete for test fixture");
    loop {
        let report = db
            .db_ops()
            .atoms()
            .reindex_atom_ref_edges(None, Some(256))
            .await
            .expect("complete atom reverse edges for test fixture");
        if report.completed {
            break;
        }
    }
}

/// Write the pre-hard-delete on-disk shape at each key: a tombstone atom in
/// EVERY field of the schema, exactly as the repurposed `MutationType::Delete`
/// wrote it before delete was routed through the purge path
/// (`north-star-lastdb-delete-returns-the-bytes` slice A).
///
/// `Delete` now erases, so no supported write path produces a tombstone any
/// more. The population is not gone, though — tens of thousands are already on
/// disk on real stores, and every read surface must keep filtering them until
/// the one-shot drain lands. A test that needs a tombstone-heavy store must
/// therefore seed one directly; the alternative is what actually happened when
/// slice A landed, which is that four paging tests kept their names, stopped
/// having any tombstone to page past, and passed against an ordinary store.
///
/// Seeding through the ordinary atom path rather than raw store writes is what
/// makes the shape faithful: `KeyMetadata.tombstoned` is derived at write time
/// from the atom content (`field/variant/write.rs`), so the flag lands the same
/// way it did then, and so does the `mk:` record per field.
///
/// Returns the number of `mk:` records tombstoned — `keys.len() * fields`.
pub async fn seed_legacy_tombstones(
    db: &crate::fold_db_core::FoldDB,
    schema_name: &str,
    keys: &[crate::schema::types::KeyValue],
) -> u64 {
    use crate::atom::tombstone_content;
    use crate::schema::types::operations::MutationType;
    use crate::schema::types::Mutation;

    let schema = db
        .schema_manager()
        .get_schema_metadata(schema_name)
        .expect("schema metadata")
        .unwrap_or_else(|| panic!("schema '{schema_name}' is not registered"));
    let field_names: Vec<String> = schema.runtime_fields.keys().cloned().collect();

    let mut mutations = Vec::with_capacity(keys.len());
    for key_value in keys {
        // Same key rendering `build_delete_tombstone_fields` used, so the
        // seeded content is byte-identical to what the old delete wrote.
        let key_repr = match (&key_value.hash, &key_value.range) {
            (Some(h), Some(r)) => format!("{h}/{r}"),
            (Some(h), None) => h.clone(),
            (None, Some(r)) => r.clone(),
            (None, None) => String::new(),
        };
        let fields: HashMap<String, serde_json::Value> = field_names
            .iter()
            .map(|field_name| {
                (
                    field_name.clone(),
                    tombstone_content(&key_repr, "user-requested", "<unknown>"),
                )
            })
            .collect();
        mutations.push(Mutation::new(
            schema_name.to_string(),
            fields,
            key_value.clone(),
            "pk".to_string(),
            MutationType::Update,
        ));
    }

    let written = (mutations.len() * field_names.len()) as u64;
    db.mutation_manager()
        .write_mutations_batch_async(mutations, None)
        .await
        .expect("seed legacy tombstones");
    written
}

/// Rewrite every `mk:` record with `meta.tombstoned` cleared, leaving atom
/// content untouched — the on-disk shape a binary older than the flag produced.
///
/// `KeyMetadata.tombstoned` is `#[serde(default)]`, so those records read back
/// as "live" while their atom content is a tombstone. This makes a store that
/// exercises that population without needing an old binary to write it.
/// Returns the number of records whose flag was cleared.
pub async fn clear_key_tombstone_flags(db: &crate::fold_db_core::FoldDB) -> u64 {
    use crate::db_operations::atom_store::PerKeyRecord;

    let store = db.db_ops().atoms();
    let rows = store
        .raw()
        .inner()
        .scan_prefix(b"mk:")
        .await
        .expect("scan mk records");
    let mut rewrites: Vec<(String, serde_json::Value)> = Vec::new();
    for (k, v) in rows {
        let Ok(mut rec) = serde_json::from_slice::<PerKeyRecord>(&v) else {
            continue;
        };
        if !rec.meta.as_ref().is_some_and(|m| m.tombstoned) {
            continue;
        }
        if let Some(meta) = rec.meta.as_mut() {
            meta.tombstoned = false;
        }
        rewrites.push((
            String::from_utf8_lossy(&k).into_owned(),
            serde_json::to_value(&rec).expect("serialize legacy tip"),
        ));
    }
    let cleared = rewrites.len() as u64;
    if !rewrites.is_empty() {
        store
            .raw()
            .batch_put_items(rewrites)
            .await
            .expect("write legacy tips");
    }
    cleared
}

/// Delete the atom BODY for `atom_uuid` while leaving every tip that points at
/// it in place — the on-disk shape a dangling tip -> atom edge has.
///
/// This is the population behind the 951 tips the atom partition-prefix rekey
/// could not resolve, and behind the two `lastdb_telemetry` keys that made
/// their whole partition unreadable. Reproducing it needs raw store access
/// because no supported write path can produce it.
///
/// Matches under BOTH atom key encodings: `Flat` stores `atom:{uuid}`, while
/// `PartitionPrefix` stores `atom:{partition}{uuid}`, so keying off
/// [`crate::atom::atom_key_codec::flat_key`] alone silently deletes nothing on
/// a partition-prefixed home. Returns the number of body keys removed.
pub async fn orphan_atom_body(db: &crate::fold_db_core::FoldDB, atom_uuid: &str) -> u64 {
    use crate::atom::atom_key_codec;

    let store = db.db_ops().atoms();
    let rows = store
        .raw()
        .inner()
        .scan_prefix(b"atom:")
        .await
        .expect("scan atom bodies");

    let mut removed = 0u64;
    for (k, _) in rows {
        let key = String::from_utf8_lossy(&k).into_owned();
        if atom_key_codec::uuid_of(&key) != Some(atom_uuid) {
            continue;
        }
        if store
            .raw()
            .delete_item(&key)
            .await
            .expect("delete atom body")
        {
            removed += 1;
        }
    }
    removed
}

/// Builder for test schemas with automatic field classification.
/// Every field gets a DataClassification so schemas pass validation.
pub struct TestSchemaBuilder {
    name: String,
    descriptive_name: Option<String>,
    fields: Vec<String>,
    hash_field: Option<String>,
    range_field: Option<String>,
    sensitivity: u8,
    data_domain: String,
    field_classifications: HashMap<String, (u8, String)>,
    field_mappers: HashMap<String, String>,
    field_types: HashMap<String, serde_json::Value>,
    ref_fields: HashMap<String, String>,
}

impl TestSchemaBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            descriptive_name: None,
            fields: Vec::new(),
            hash_field: None,
            range_field: None,
            sensitivity: 0,
            data_domain: "general".to_string(),
            field_classifications: HashMap::new(),
            field_mappers: HashMap::new(),
            field_types: HashMap::new(),
            ref_fields: HashMap::new(),
        }
    }

    pub fn descriptive_name(mut self, name: &str) -> Self {
        self.descriptive_name = Some(name.to_string());
        self
    }

    fn ensure_field(&mut self, name: &str) {
        let s = name.to_string();
        if !self.fields.contains(&s) {
            self.fields.push(s);
        }
    }

    pub fn field(mut self, name: &str) -> Self {
        self.ensure_field(name);
        self
    }

    pub fn fields(mut self, names: &[&str]) -> Self {
        for name in names {
            self.ensure_field(name);
        }
        self
    }

    pub fn range_key(mut self, field: &str) -> Self {
        self.range_field = Some(field.to_string());
        self.ensure_field(field);
        self
    }

    pub fn hash_key(mut self, field: &str) -> Self {
        self.hash_field = Some(field.to_string());
        self.ensure_field(field);
        self
    }

    /// Set default sensitivity for all fields (0=Public, 4=HighlyRestricted)
    pub fn sensitivity(mut self, level: u8) -> Self {
        self.sensitivity = level;
        self
    }

    /// Set default data domain for all fields
    pub fn domain(mut self, domain: &str) -> Self {
        self.data_domain = domain.to_string();
        self
    }

    /// Override classification for a specific field
    pub fn classify(mut self, field: &str, sensitivity: u8, domain: &str) -> Self {
        self.field_classifications
            .insert(field.to_string(), (sensitivity, domain.to_string()));
        self
    }

    /// Add a field mapper (e.g. "id" -> "User.id")
    pub fn field_mapper(mut self, field: &str, source: &str) -> Self {
        self.field_mappers
            .insert(field.to_string(), source.to_string());
        self
    }

    /// Add a typed field (e.g. "age" -> json!("Integer"))
    pub fn field_type(mut self, field: &str, typ: serde_json::Value) -> Self {
        self.field_types.insert(field.to_string(), typ);
        self
    }

    /// Add a ref field (e.g. "posts" -> "Post")
    pub fn ref_field(mut self, field: &str, target_schema: &str) -> Self {
        self.ref_fields
            .insert(field.to_string(), target_schema.to_string());
        self
    }

    /// Build the schema as a JSON string suitable for load_schema_from_json
    pub fn build_json(&self) -> String {
        let mut classifications = serde_json::Map::new();
        for field in &self.fields {
            let (sens, domain) = self
                .field_classifications
                .get(field)
                .cloned()
                .unwrap_or((self.sensitivity, self.data_domain.clone()));
            classifications.insert(
                field.clone(),
                json!({
                    "sensitivity_level": sens,
                    "data_domain": domain
                }),
            );
        }

        let mut key = serde_json::Map::new();
        if let Some(ref h) = self.hash_field {
            key.insert("hash_field".to_string(), json!(h));
        }
        if let Some(ref r) = self.range_field {
            key.insert("range_field".to_string(), json!(r));
        }

        let mut fields_map = serde_json::Map::new();
        for field in &self.fields {
            fields_map.insert(field.clone(), json!({}));
        }

        let mut schema = json!({
            "name": self.name,
            "fields": fields_map,
            "field_data_classifications": classifications,
        });

        if !key.is_empty() {
            schema
                .as_object_mut()
                .unwrap()
                .insert("key".to_string(), serde_json::Value::Object(key));
        }

        if let Some(ref dn) = self.descriptive_name {
            schema
                .as_object_mut()
                .unwrap()
                .insert("descriptive_name".to_string(), json!(dn));
        }

        if !self.field_mappers.is_empty() {
            schema
                .as_object_mut()
                .unwrap()
                .insert("field_mappers".to_string(), json!(self.field_mappers));
        }

        if !self.field_types.is_empty() {
            schema
                .as_object_mut()
                .unwrap()
                .insert("field_types".to_string(), json!(self.field_types));
        }

        if !self.ref_fields.is_empty() {
            schema
                .as_object_mut()
                .unwrap()
                .insert("ref_fields".to_string(), json!(self.ref_fields));
        }

        serde_json::to_string_pretty(&schema).unwrap()
    }
}
