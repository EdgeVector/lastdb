//! Complete logical membership with one shared, bounded first-use loader.
use super::*;
use crate::db_operations::DbOperations;
use crate::resident::core_keys::{
    CoreKeyBudget, CoreKeyDomain, CoreKeyIdentity, CoreKeyLease, CoreKeyReservation,
};
use crate::schema::{Schema, SchemaError};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicUsize;

mod binding;
mod load;
mod publication;
mod selection;

type DomainId = (Option<String>, String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Unloaded,
    Loading,
    Ready,
    DiskOnly,
}

struct EntryState {
    domain: Option<Arc<CoreKeyDomain>>,
    has_generation: Option<bool>,
    changes: BTreeMap<String, CoreKeyReservation>,
    reason: Option<String>,
}

struct Entry {
    id: DomainId,
    phase: tokio::sync::watch::Sender<Phase>,
    state: Mutex<EntryState>,
    tick: AtomicU64,
    _charge: CoreKeyReservation,
}

pub(super) struct SchemaKeys {
    policy: Vec<String>,
    budget: Arc<CoreKeyBudget>,
    entries: Mutex<HashMap<DomainId, Arc<Entry>>>,
    loads: AtomicU64,
    hits: AtomicU64,
    clock: AtomicU64,
    active: AtomicUsize,
    load_slots: Arc<tokio::sync::Semaphore>,
}

#[derive(Default, serde::Serialize)]
pub struct SchemaKeyStatus {
    pub enabled: bool,
    pub domains: usize,
    pub ready: usize,
    pub loading: usize,
    pub disk_only: usize,
    /// Retained for the status contract; refused preloads use disk_only.
    pub failed: usize,
    pub keys: usize,
    pub loads: u64,
    pub memory_selections: u64,
    pub bytes: usize,
    pub peak_bytes: usize,
    pub limit_bytes: usize,
    pub reasons: Vec<String>,
}

impl SchemaKeys {
    pub(super) fn new() -> Self {
        Self {
            policy: std::env::var("LASTDB_SCHEMA_PRELOAD_SCHEMAS")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
            budget: Arc::new(CoreKeyBudget::from_env()),
            entries: Mutex::new(HashMap::new()),
            loads: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            clock: AtomicU64::new(0),
            active: AtomicUsize::new(0),
            load_slots: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    fn selected(&self, schema: &Schema) -> bool {
        self.policy.iter().any(|name| {
            name == "*" || name == &schema.name || schema.identity_hash.as_ref() == Some(name)
        })
    }

    fn entry(&self, id: DomainId) -> Option<Arc<Entry>> {
        let mut entries = self.entries.lock().expect("schema key entries");
        if let Some(entry) = entries.get(&id) {
            return Some(Arc::clone(entry));
        }
        if entries.len() >= 1024 {
            return None;
        }
        let entry = Arc::new(Entry {
            id: id.clone(),
            phase: tokio::sync::watch::channel(Phase::Unloaded).0,
            state: Mutex::new(EntryState {
                domain: None,
                has_generation: None,
                changes: BTreeMap::new(),
                reason: None,
            }),
            tick: AtomicU64::new(0),
            _charge: self
                .budget
                .reserve_temporary(1024 + id.1.len() + id.0.as_ref().map_or(0, String::len))
                .ok()?,
        });
        entries.insert(id, Arc::clone(&entry));
        Some(entry)
    }

    pub(super) fn status(&self) -> SchemaKeyStatus {
        let budget = self.budget.stats();
        let entries = self.entries.lock().expect("schema key status");
        let mut status = SchemaKeyStatus {
            enabled: !self.policy.is_empty(),
            domains: entries.len(),
            loads: self.loads.load(Ordering::Relaxed),
            memory_selections: self.hits.load(Ordering::Relaxed),
            bytes: budget.used_bytes,
            peak_bytes: budget.peak_bytes,
            limit_bytes: budget.limit_bytes,
            ..SchemaKeyStatus::default()
        };
        for entry in entries.values() {
            match *entry.phase.borrow() {
                Phase::Ready => status.ready += 1,
                Phase::Loading => status.loading += 1,
                Phase::DiskOnly => status.disk_only += 1,
                Phase::Unloaded => {}
            }
            let state = entry.state.lock().expect("schema key state");
            if let Some(domain) = &state.domain {
                status.keys += domain.stats().map_or(0, |stats| stats.keys);
            }
            if let Some(reason) = &state.reason {
                if !status.reasons.contains(reason) {
                    status.reasons.push(reason.clone());
                }
            }
        }
        status
    }
}

impl PackRuntime {
    /// Explicit selected-schema policy: the primary field molecule keys.
    /// A fallback field loads its different membership only when required.
    pub(crate) async fn preload_schema_keys(
        self: &Arc<Self>,
        db_ops: Arc<DbOperations>,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        if !self.schema_keys.selected(schema) || transaction::PendingBatch::current(self).is_some()
        {
            return Ok(());
        }
        let names = schema.runtime_field_names();
        let primary = schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.as_ref())
            .filter(|name| schema.runtime_fields.contains_key(name.as_str()))
            .or_else(|| names.first());
        let Some(field) = primary.and_then(|name| schema.runtime_fields.get(name)) else {
            return Ok(());
        };
        if field.kind != crate::schema::types::field::FieldKind::HashRange {
            return Ok(());
        }
        let Some(molecule) = field.common().molecule_uuid() else {
            return Ok(());
        };
        let scope = field.common().storage_prefix();
        self.preload_molecule_keys(db_ops, scope, molecule).await?;
        Ok(())
    }

    pub(crate) async fn preload_molecule_keys(
        self: &Arc<Self>,
        db_ops: Arc<DbOperations>,
        scope: Option<&str>,
        molecule: &str,
    ) -> Result<(), SchemaError> {
        // Key identity must be established before an empty or cold domain loads.
        db_ops.atoms().load_molecule_key_bundle(molecule).await?;
        let prefix = crate::atom::molecule_key_codec::molecule_record_prefix(molecule);
        let Some(uid) = crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(&prefix)
        else {
            return Err(SchemaError::InvalidData("invalid schema key domain".into()));
        };
        let Some(entry) = self
            .schema_keys
            .entry((scope.map(str::to_owned), uid.to_owned()))
        else {
            return Ok(());
        };
        self.check_key_encoding(&db_ops, &entry)?;
        self.ensure_key_entry(db_ops, entry).await?;
        Ok(())
    }
}

fn encoding_identity(db_ops: &DbOperations, molecule: &str) -> Result<String, SchemaError> {
    let codec = db_ops.atoms().key_codec_for_molecule(molecule)?;
    // Sentinel outputs identify the fixed molecule key pair without
    // retaining encryption keys. They are never used as database coordinates.
    Ok(format!(
        "{:?}/{:?}/{:?}/{:?}",
        codec.encoding(),
        codec.range_encoding(),
        codec
            .storage_hash(molecule, "schema-preload-identity")
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?,
        codec
            .storage_range(molecule, "\0")
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?
    ))
}

fn key_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::ReadResourceLimit {
        detail: format!("schema key preload: {error}"),
    }
}
