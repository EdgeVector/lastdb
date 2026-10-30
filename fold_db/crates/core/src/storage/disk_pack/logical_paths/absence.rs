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
    if !field::provenance_admits(&runtime) || candidates.is_empty() {
        return Ok(());
    }
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(());
    };
    let sources = dependencies::tip_sources(scope, molecule, candidates);
    let path = binding.path(scope, molecule, key, "").core();
    let Some(mut held) = super::lock_current_model(
        &runtime,
        &context,
        "missing key captured view",
        "missing key core model",
    ) else {
        return Ok(());
    };
    context.hold_core(&mut held.core, std::slice::from_ref(&path))?;
    held.core
        .admit_absent(&path)
        .map_err(|error| StorageError::ReadResourceLimit {
            detail: error.to_string(),
        })?;
    if sources
        .iter()
        .all(|(collection, key)| held.core.link_source(&path, collection, key))
    {
        held.core.authorize_path(&path);
    } else {
        held.core.invalidate_slot(&path);
    }
    Ok(())
}
