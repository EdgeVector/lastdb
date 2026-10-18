//! Existing filters borrow the actual warm molecule for fully known exact keys.
use super::*;
use crate::schema::types::field::{
    apply_keyed_filter, FieldKind, HashRangeFilter, KeyedAtomMatch, KeyedFilterMode,
};
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::read::ReadContext;

pub(crate) fn matches(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: Option<&str>,
    kind: FieldKind,
    filter: &HashRangeFilter,
    include_tombstones: bool,
) -> StorageResult<Option<Vec<KeyedAtomMatch>>> {
    let Some((runtime, binding, molecule)) = runtime
        .zip(field::FieldScope::current())
        .zip(molecule)
        .map(|((r, b), m)| (r, b, m))
    else {
        return Ok(None);
    };
    if !field::provenance_current() || transaction::PendingBatch::current(&runtime).is_some() {
        return Ok(None);
    }
    let Some(keys) = exact_keys(kind, filter) else {
        return Ok(None);
    };
    let context = ReadContext::captured(&runtime).ok_or_else(|| {
        StorageError::InvalidOperation("core filter requires captured query".into())
    })?;
    let view = runtime.view.read().expect("core filter captured view");
    if view.revision != context.revision {
        return Ok(None);
    }
    let paths: Vec<_> = keys
        .iter()
        .map(|key| binding.path(scope, molecule, key, "").core())
        .collect();
    let mut call = crate::read_receipts::lock(&context.call);
    let mut core = runtime.core.lock().expect("core filter model");
    if paths.iter().any(|path| !core.path_authorized(path)) {
        return Ok(None);
    }
    context.hold_core(&mut core, &paths)?;
    let mode = if kind == FieldKind::Hash {
        KeyedFilterMode::Hash
    } else {
        KeyedFilterMode::HashRange
    };
    let filtered = core.with_molecule(scope, molecule, |model| {
        apply_keyed_filter(model, Some(filter.clone()), mode)
    });
    let mut matches = Vec::new();
    for key in keys {
        let path = binding.path(scope, molecule, &key, "").core();
        let value = core.lookup(&path);
        if value.is_unknown() {
            return Ok(None);
        }
        if value.is_absent() {
            context.account_memory_record(
                &mut call,
                key.hash.as_ref().map_or(0, String::len)
                    + key.range.as_ref().map_or(0, String::len),
                0,
            )?;
            crate::read_receipts::warm_hit();
            continue;
        }
        let Some(value) = value.into_tip() else {
            return Ok(None);
        };
        if filtered
            .as_ref()
            .is_none_or(|filtered| !filtered.matches.contains_key(&key))
        {
            continue;
        }
        if !include_tombstones && value.metadata.as_ref().is_some_and(|meta| meta.tombstoned) {
            continue;
        }
        let Some(body) = source::body_location(&core, &path, &value.entry.atom_uuid)? else {
            return Ok(None);
        };
        let writer = (!value.entry.writer_pubkey.is_empty()).then_some(value.entry.writer_pubkey);
        matches.push((
            key,
            value.entry.atom_uuid,
            value.metadata,
            writer,
            body.partition,
        ));
    }
    Ok(Some(matches))
}

fn exact_keys(kind: FieldKind, filter: &HashRangeFilter) -> Option<Vec<KeyValue>> {
    match (kind, filter) {
        (FieldKind::Hash, HashRangeFilter::HashKey(hash)) => {
            Some(vec![KeyValue::new(Some(hash.clone()), None)])
        }
        (FieldKind::Hash, HashRangeFilter::HashRangeKeys(keys)) => Some(
            keys.iter()
                .map(|(hash, _)| KeyValue::new(Some(hash.clone()), None))
                .collect(),
        ),
        (FieldKind::HashRange, HashRangeFilter::HashRangeKey { hash, range }) => {
            Some(vec![KeyValue::new(Some(hash.clone()), Some(range.clone()))])
        }
        (FieldKind::HashRange, HashRangeFilter::HashRangeKeys(keys)) => Some(
            keys.iter()
                .map(|(hash, range)| KeyValue::new(Some(hash.clone()), Some(range.clone())))
                .collect(),
        ),
        _ => None,
    }
}
