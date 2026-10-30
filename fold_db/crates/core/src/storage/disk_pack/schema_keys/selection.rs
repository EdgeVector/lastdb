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
        let (_context, _view) = captured_selection_view(self)?;
        Some(select())
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
        let Some(held) = open_current_selection(self, scope, molecule, key)? else {
            return Ok(false);
        };
        let mut core = self.core().lock().map_err(|_| selection_lock_error())?;
        if !core.path_authorized(&held.path) {
            return Ok(false);
        }
        let Some(selected) = core.lookup(&held.path).into_tip() else {
            return Ok(false);
        };
        if selected.entry != expected.entry || selected.metadata != expected.meta {
            return Ok(false);
        }
        let sources = core.source_coordinates(&held.path);
        drop(core);
        drop(held);
        if !super::super::logical_paths::source::body_present_in_coordinates(
            &sources,
            &expected.entry.atom_uuid,
        )? {
            return Ok(false);
        }
        super::super::logical_paths::core_read::note_core_tip(
            Some(Arc::clone(self)),
            scope,
            molecule,
            key,
            &expected.entry,
            expected.meta.as_ref(),
        )
    }

    /// A query retains this same domain through every key and value chunk.
    pub(crate) fn schema_key_lease(
        self: &Arc<Self>,
        scope: Option<&str>,
        molecule: &str,
    ) -> Option<CoreKeyLease> {
        let (_context, _view) = captured_selection_view(self)?;
        if let Some(entry) = self
            .schema_keys()
            .entries
            .lock()
            .ok()?
            .get(&(scope.map(str::to_owned), molecule.to_owned()))
        {
            entry.tick.store(
                self.schema_keys().clock.fetch_add(1, Ordering::Relaxed),
                Ordering::Relaxed,
            );
        }
        let tag = std::ptr::from_ref(&KEY_LEASES) as usize;
        let name = serde_json::to_string(&(scope, molecule)).ok()?;
        if let Some(held) =
            read_snapshot_get::<CoreKeyLease>(Some(Arc::clone(self)), tag, &name).ok()?
        {
            self.schema_keys().hits.fetch_add(1, Ordering::Relaxed);
            return Some(held.as_ref().clone());
        }
        let lease = self.core().lock().ok()?.key_lease(scope, molecule)?;
        let held = read_snapshot_insert(Some(Arc::clone(self)), tag, name, lease).ok()?;
        self.schema_keys().hits.fetch_add(1, Ordering::Relaxed);
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
        let Some(held) = open_current_selection(self, scope, molecule, api_key)? else {
            return Ok(None);
        };
        let mut call = crate::read_receipts::lock(&held.context.call);
        let mut core = self.core().lock().map_err(|_| selection_lock_error())?;
        if !core.path_authorized(&held.path) {
            return Ok(None);
        }
        let selection = core.lookup(&held.path);
        if selection.is_unknown() {
            return Ok(None);
        }
        if selection.is_absent() {
            held.context.account_memory_record(
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
        if !super::super::logical_paths::source::body_present(
            &core,
            &held.path,
            &selection.entry.atom_uuid,
        )? {
            return Ok(None);
        }
        held.context.account_memory_record(
            &mut call,
            held.path.hash.len() + held.path.range.len(),
            tip_metadata_bytes(&selection.entry, selection.metadata.as_ref())?,
        )?;
        if !super::super::logical_paths::core_read::note_core_tip(
            Some(Arc::clone(self)),
            scope,
            molecule,
            api_key,
            &selection.entry,
            selection.metadata.as_ref(),
        )? {
            return Ok(None);
        }
        Ok(Some(Some(PerKeyRecord {
            entry: selection.entry,
            meta: selection.metadata,
        })))
    }
}

/// The published view for one matching revision, and the core path.
/// A newer view returns none and does not take the core lock.
struct CurrentSelection<'a> {
    context: std::sync::Arc<ReadContext>,
    path: crate::resident::CorePath,
    _view: std::sync::RwLockReadGuard<'a, super::super::view::NativeReadView>,
}

fn open_current_selection<'a>(
    runtime: &'a PackRuntime,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
) -> StorageResult<Option<CurrentSelection<'a>>> {
    if !field::provenance_admits(runtime) {
        return Ok(None);
    }
    let Some(context) = ReadContext::captured(runtime) else {
        return Ok(None);
    };
    let Some(binding) = field::FieldScope::current() else {
        return Ok(None);
    };
    let view = runtime.view.read().map_err(|_| selection_lock_error())?;
    if view.revision != context.revision {
        return Ok(None);
    }
    let path = binding.path(scope, molecule, key, "").core();
    Ok(Some(CurrentSelection {
        context,
        path,
        _view: view,
    }))
}

/// A pending batch, stale provenance, a poisoned view, or a newer view returns none.
/// The caller holds both values, so the view lock stays through the rest of the call.
fn captured_selection_view(
    runtime: &PackRuntime,
) -> Option<(
    Arc<ReadContext>,
    std::sync::RwLockReadGuard<'_, super::super::view::NativeReadView>,
)> {
    if !field::provenance_admits(runtime) {
        return None;
    }
    let context = ReadContext::captured(runtime)?;
    let view = runtime.view.read().ok()?;
    (view.revision == context.revision).then_some((context, view))
}

fn selection_lock_error() -> StorageError {
    StorageError::BackendError("complete key selection lock is unavailable".into())
}

fn tip_metadata_bytes(
    entry: &crate::atom::AtomEntry,
    metadata: Option<&crate::atom::KeyMetadata>,
) -> StorageResult<usize> {
    serde_json::to_vec(&(entry, metadata))
        .map(|bytes| bytes.len())
        .map_err(|_| {
            StorageError::SerializationError("native tip metadata byte accounting failed".into())
        })
}
