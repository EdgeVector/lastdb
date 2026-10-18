//! Complete-key lifetimes and authorized tips belong to one captured query.

use super::*;
use crate::db_operations::atom_store::PerKeyRecord;
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::logical_paths::field;
use crate::storage::disk_pack::read::ReadContext;

static KEY_LEASES: u8 = 0;

impl PackRuntime {
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
        let Some(binding) = field::FieldScope::current() else {
            return Ok(None);
        };
        let Some(context) = ReadContext::captured(self) else {
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
}

fn selection_lock_error() -> StorageError {
    StorageError::BackendError("complete key selection lock is unavailable".into())
}
