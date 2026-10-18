//! Complete-key lifetimes and authorized tips belong to one captured query.

use super::*;
use crate::db_operations::atom_store::PerKeyRecord;
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::logical_paths::field;
use crate::storage::disk_pack::read::ReadContext;

static KEY_LEASES: u8 = 0;

impl PackRuntime {
    /// Domain membership remains mutable, so selection holds the view boundary.
    pub(crate) fn with_schema_key_selection<T>(&self, select: impl FnOnce() -> T) -> Option<T> {
        if transaction::PendingBatch::current(self).is_some() || !field::provenance_current() {
            return None;
        }
        let context = ReadContext::captured(self)?;
        let view = self.view.read().ok()?;
        (view.revision == context.revision).then(select)
    }

    pub(crate) fn captured_core_model(&self) -> Option<crate::resident::CoreModelSnapshot> {
        if transaction::PendingBatch::current(self).is_some() {
            return None;
        }
        Some(ReadContext::captured(self)?.core_model.clone())
    }

    /// A selected row can keep the body's real location, including a protein
    /// sibling's copy. A mismatched row never changes the selected-tip receipt.
    pub(crate) fn schema_key_tip_for_record(
        self: &Arc<Self>,
        scope: Option<&str>,
        molecule: &str,
        key: &KeyValue,
        expected: &PerKeyRecord,
    ) -> StorageResult<bool> {
        if transaction::PendingBatch::current(self).is_some() || !field::provenance_current() {
            return Ok(false);
        }
        let Some(context) = ReadContext::captured(self) else {
            return Ok(false);
        };
        let path = crate::resident::CorePath {
            scope: scope.map(str::to_owned),
            schema: String::new(),
            field: String::new(),
            molecule: molecule.to_owned(),
            hash: key.hash.clone().unwrap_or_default(),
            range: key.range.clone().unwrap_or_default(),
        };
        let captured = context.core_model.lookup(&path);
        let sources = if captured.is_unknown() {
            let Some(binding) = field::FieldScope::current() else {
                return Ok(false);
            };
            let view = self.view.read().map_err(|_| selection_lock_error())?;
            if view.revision != context.revision {
                return Ok(false);
            }
            let path = binding.path(scope, molecule, key, "").core();
            let mut core = self.core.lock().map_err(|_| selection_lock_error())?;
            if !core.path_authorized(&path) {
                return Ok(false);
            }
            context.hold_core(&mut core, std::slice::from_ref(&path))?;
            let Some(selected) = core.lookup(&path).into_tip() else {
                return Ok(false);
            };
            if selected.entry != expected.entry || selected.metadata != expected.meta {
                return Ok(false);
            }
            core.source_coordinates(&path)
        } else {
            let Some(selected) = captured.into_tip() else {
                return Ok(false);
            };
            if selected.entry != expected.entry || selected.metadata != expected.meta {
                return Ok(false);
            }
            context.core_model.source_coordinates(&path)
        };
        let Some(body) = super::super::logical_paths::source::body_location_from_coordinates(
            &sources,
            &expected.entry.atom_uuid,
        )?
        else {
            return Ok(false);
        };
        super::super::logical_paths::core_read::note_partition(
            Some(Arc::clone(self)),
            scope,
            molecule,
            key,
            &expected.entry,
            expected.meta.as_ref(),
            body.partition.as_ref(),
        )
    }

    /// A query retains this same domain through every key and value chunk.
    pub(crate) fn schema_key_lease(
        self: &Arc<Self>,
        scope: Option<&str>,
        molecule: &str,
    ) -> Option<CoreKeyLease> {
        if transaction::PendingBatch::current(self).is_some() || !field::provenance_current() {
            return None;
        }
        let context = ReadContext::captured(self)?;
        let view = self.view.read().ok()?;
        if view.revision != context.revision {
            return None;
        }
        if let Some(entry) = self
            .schema_keys
            .entries
            .lock()
            .ok()?
            .get(&(scope.map(str::to_owned), molecule.to_owned()))
        {
            entry.tick.store(
                self.schema_keys.clock.fetch_add(1, Ordering::Relaxed),
                Ordering::Relaxed,
            );
        }
        let tag = std::ptr::from_ref(&KEY_LEASES) as usize;
        let name = serde_json::to_string(&(scope, molecule)).ok()?;
        if let Some(held) =
            read_snapshot_get::<CoreKeyLease>(Some(Arc::clone(self)), tag, &name).ok()?
        {
            self.schema_keys.hits.fetch_add(1, Ordering::Relaxed);
            return Some(held.as_ref().clone());
        }
        let lease = self.core.lock().ok()?.key_lease(scope, molecule)?;
        let held = read_snapshot_insert(Some(Arc::clone(self)), tag, name, lease).ok()?;
        self.schema_keys.hits.fetch_add(1, Ordering::Relaxed);
        Some(held.as_ref().clone())
    }

    /// Reuse full tip metadata only while its ordinary core source authority
    /// and this query's common publication boundary remain current.
    pub(crate) fn schema_key_tip(
        self: &Arc<Self>,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
    ) -> StorageResult<Option<Option<PerKeyRecord>>> {
        if transaction::PendingBatch::current(self).is_some() || !field::provenance_current() {
            return Ok(None);
        }
        let Some(context) = ReadContext::captured(self) else {
            return Ok(None);
        };
        if let Some(value) = self.snapshot_key_tip(&context, scope, molecule, api_key)? {
            return Ok(Some(value));
        }
        let Some(binding) = field::FieldScope::current() else {
            return Ok(None);
        };
        let view = self.view.read().map_err(|_| selection_lock_error())?;
        if view.revision != context.revision {
            return Ok(None);
        }
        let path = binding.path(scope, molecule, api_key, "").core();
        let mut call = crate::read_receipts::lock(&context.call);
        let mut core = self.core.lock().map_err(|_| selection_lock_error())?;
        if !core.path_authorized(&path) {
            return Ok(None);
        }
        context.hold_core(&mut core, std::slice::from_ref(&path))?;
        let selection = core.lookup(&path);
        if selection.is_unknown() {
            return Ok(None);
        }
        if selection.is_absent() {
            context.account_memory_record(
                &mut call,
                api_key.hash.as_ref().map_or(0, String::len)
                    + api_key.range.as_ref().map_or(0, String::len),
                0,
            )?;
            crate::read_receipts::warm_hit();
            return Ok(Some(None));
        }
        let Some(selection) = selection.into_tip() else {
            return Ok(None);
        };
        let Some(body) = super::super::logical_paths::source::body_location(
            &core,
            &path,
            &selection.entry.atom_uuid,
        )?
        else {
            return Ok(None);
        };
        if !super::super::logical_paths::core_read::note_partition(
            Some(Arc::clone(self)),
            scope,
            molecule,
            api_key,
            &selection.entry,
            selection.metadata.as_ref(),
            body.partition.as_ref(),
        )? {
            return Ok(None);
        }
        Ok(Some(Some(PerKeyRecord {
            entry: selection.entry,
            meta: selection.metadata,
        })))
    }

    fn snapshot_key_tip(
        self: &Arc<Self>,
        context: &Arc<ReadContext>,
        scope: Option<&str>,
        molecule: &str,
        key: &KeyValue,
    ) -> StorageResult<Option<Option<PerKeyRecord>>> {
        let path = crate::resident::CorePath {
            scope: scope.map(str::to_owned),
            schema: String::new(),
            field: String::new(),
            molecule: molecule.to_owned(),
            hash: key.hash.clone().unwrap_or_default(),
            range: key.range.clone().unwrap_or_default(),
        };
        let selected = context.core_model.lookup(&path);
        if selected.is_unknown() {
            return Ok(None);
        }
        if selected.is_absent() {
            context.account_memory_record(
                &mut crate::read_receipts::lock(&context.call),
                path.hash.len() + path.range.len(),
                0,
            )?;
            crate::read_receipts::warm_hit();
            return Ok(Some(None));
        }
        let selected = selected.into_tip().expect("known positive typed tip");
        let location = super::super::logical_paths::source::body_location_from_coordinates(
            &context.core_model.source_coordinates(&path),
            &selected.entry.atom_uuid,
        )?
        .ok_or_else(selection_lock_error)?;
        context.account_memory_record(
            &mut crate::read_receipts::lock(&context.call),
            path.hash.len() + path.range.len(),
            selected.bytes,
        )?;
        if !super::super::logical_paths::core_read::note_partition(
            Some(Arc::clone(self)),
            scope,
            molecule,
            key,
            &selected.entry,
            selected.metadata.as_ref(),
            location.partition.as_ref(),
        )? {
            return Err(StorageError::ReadResourceLimit {
                detail: "captured core tip cannot retain its atom location".into(),
            });
        }
        crate::read_receipts::warm_hit();
        Ok(Some(Some(PerKeyRecord {
            entry: selected.entry,
            meta: selected.metadata,
        })))
    }
}

fn selection_lock_error() -> StorageError {
    StorageError::BackendError("complete key selection lock is unavailable".into())
}
