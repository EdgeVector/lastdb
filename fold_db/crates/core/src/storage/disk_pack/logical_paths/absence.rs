//! Confirmed missing keys use the same held logical paths as present records.

use super::*;
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::read::ReadContext;

pub(crate) fn admit(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    candidates: &[String],
) -> StorageResult<()> {
    let Some(binding) = field::FieldScope::current() else {
        return Ok(());
    };
    admit_bound(runtime, &binding, scope, molecule, key, candidates)
}

pub(super) fn admit_bound(
    runtime: Option<Arc<PackRuntime>>,
    binding: &field::FieldScope,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    candidates: &[String],
) -> StorageResult<()> {
    let Some(runtime) = runtime else {
        return Ok(());
    };
    if !field::provenance_current()
        || transaction::PendingBatch::current(&runtime).is_some()
        || candidates.is_empty()
    {
        return Ok(());
    }
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(());
    };
    let sources = dependencies::tip_sources(scope, molecule, candidates);
    let path = binding.path(scope, molecule, key, "").core();
    let view = runtime.view.read().expect("missing key captured view");
    if view.revision != context.revision {
        return Ok(());
    }
    let mut core = runtime.core.lock().expect("missing key core model");
    context.hold_core(&mut core, std::slice::from_ref(&path))?;
    core.admit_absent(&path)
        .map_err(|error| StorageError::ReadResourceLimit {
            detail: error.to_string(),
        })?;
    if sources
        .iter()
        .all(|(collection, key)| core.link_source(&path, collection, key))
    {
        core.authorize_path(&path);
    } else {
        core.invalidate_slot(&path);
    }
    Ok(())
}
