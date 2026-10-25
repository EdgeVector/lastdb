//! Exact-key filters answer from warm tips for fully known keys.
use super::*;
use crate::resident::CoreWarmTip;
use crate::schema::types::field::{
    apply_keyed_filter, FieldKind, HashRangeFilter, KeyedAtomMatch, KeyedFilterMode,
};
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::physical::PhysicalRead;
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
    let paths: Vec<_> = keys
        .iter()
        .map(|key| binding.path(scope, molecule, key, "").core())
        .collect();
    let projection = Projection {
        scope,
        molecule,
        kind,
        filter,
        include_tombstones,
    };
    if let Some(matches) = captured_matches(&runtime, &context, &paths, &keys, &projection)? {
        return Ok(Some(matches));
    }
    current_matches(&runtime, &context, &paths, &keys, &projection)
}

struct Projection<'a> {
    scope: Option<&'a str>,
    molecule: &'a str,
    kind: FieldKind,
    filter: &'a HashRangeFilter,
    include_tombstones: bool,
}

fn captured_matches(
    runtime: &PackRuntime,
    context: &ReadContext,
    paths: &[CorePath],
    keys: &[KeyValue],
    projection: &Projection<'_>,
) -> StorageResult<Option<Vec<KeyedAtomMatch>>> {
    let values: Vec<_> = paths
        .iter()
        .map(|path| context.core_model.lookup(path))
        .collect();
    if values
        .iter()
        .any(crate::resident::CoreWarmLookup::is_unknown)
    {
        return Ok(None);
    }
    {
        let view = runtime.view.read().expect("captured filter path holds");
        let mut core = runtime.core.lock().expect("captured filter retention");
        context.hold_core(&mut core, paths)?;
        if view.revision == context.revision {
            for path in paths {
                let _ = core.lookup(path);
            }
        }
    }
    let mode = if projection.kind == FieldKind::Hash {
        KeyedFilterMode::Hash
    } else {
        KeyedFilterMode::HashRange
    };
    let filtered =
        context
            .core_model
            .with_molecule(projection.scope, projection.molecule, |model| {
                apply_keyed_filter(model, Some(projection.filter.clone()), mode)
            });
    let mut call = crate::read_receipts::lock(&context.call);
    let mut matches = Vec::new();
    for ((key, path), value) in keys.iter().zip(paths).zip(values) {
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
        account_tip(context, &mut call, key, &value)?;
        if filtered
            .as_ref()
            .is_none_or(|filtered| !filtered.matches.contains_key(key))
            || (!projection.include_tombstones
                && value.metadata.as_ref().is_some_and(|meta| meta.tombstoned))
        {
            continue;
        }
        if !source::body_present_in_coordinates(
            &context.core_model.source_coordinates(path),
            &value.entry.atom_uuid,
        )? {
            return Ok(None);
        }
        let writer = (!value.entry.writer_pubkey().is_empty())
            .then(|| value.entry.writer_pubkey().to_string());
        matches.push((key.clone(), value.entry.atom_uuid, value.metadata, writer));
    }
    Ok(Some(matches))
}

fn current_matches(
    runtime: &PackRuntime,
    context: &ReadContext,
    paths: &[CorePath],
    keys: &[KeyValue],
    projection: &Projection<'_>,
) -> StorageResult<Option<Vec<KeyedAtomMatch>>> {
    let view = runtime.view.read().expect("core filter captured view");
    if view.revision != context.revision {
        return Ok(None);
    }
    let mut call = crate::read_receipts::lock(&context.call);
    let mut core = runtime.core.lock().expect("core filter model");
    if paths.iter().any(|path| !core.path_authorized(path)) {
        return Ok(None);
    }
    context.hold_core(&mut core, paths)?;
    // An exact-key filter matches every listed key that has a tip, so a warm
    // tip is the whole test. The warm set keeps no molecule copy to filter.
    let mut matches = Vec::new();
    for (key, path) in keys.iter().zip(paths) {
        let value = core.lookup(path);
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
        account_tip(context, &mut call, key, &value)?;
        if !projection.include_tombstones
            && value.metadata.as_ref().is_some_and(|meta| meta.tombstoned)
        {
            continue;
        }
        if !source::body_present(&core, path, &value.entry.atom_uuid)? {
            return Ok(None);
        }
        let writer = (!value.entry.writer_pubkey().is_empty())
            .then(|| value.entry.writer_pubkey().to_string());
        matches.push((key.clone(), value.entry.atom_uuid, value.metadata, writer));
    }
    Ok(Some(matches))
}

/// Examined positive tips consume the call budget even when visibility rejects
/// them before a body fetch. Full values charge their bodies separately.
fn account_tip(
    context: &ReadContext,
    call: &mut PhysicalRead,
    key: &KeyValue,
    tip: &CoreWarmTip,
) -> StorageResult<()> {
    context.account_memory_record(
        call,
        key.hash.as_ref().map_or(0, String::len) + key.range.as_ref().map_or(0, String::len),
        body::encoded_len(&(&tip.entry, &tip.metadata))?,
    )
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
