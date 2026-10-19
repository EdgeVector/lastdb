//! Certified current tips share the logical path budget without atom values.

use super::*;
use crate::atom::atom_key_codec;
use crate::resident::CoreWarmTip;
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::read::ReadContext;

pub(super) fn admit(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    selected: &core_read::SelectedTip,
) -> StorageResult<()> {
    let Some((runtime, binding)) = runtime.zip(field::FieldScope::current()) else {
        return Ok(());
    };
    if !field::provenance_current() || transaction::PendingBatch::current(&runtime).is_some() {
        return Ok(());
    }
    let Some(sources) = &selected.sources else {
        return Ok(());
    };
    if selected
        .tip_source
        .as_ref()
        .is_none_or(|winner| !sources.contains(winner))
    {
        return Ok(());
    }
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(());
    };
    let path = binding
        .path(scope, molecule, key, &selected.entry.atom_uuid)
        .core();
    let view = runtime.view.read().expect("tip admission captured view");
    if view.revision != context.revision {
        return Ok(());
    }
    let mut core = runtime.core.lock().expect("tip admission core model");
    let Ok(preserved) = core.admit_tip(
        &path,
        CoreWarmTip {
            entry: selected.entry.clone(),
            metadata: selected.metadata.clone(),
        },
    ) else {
        return Ok(());
    };
    let body = if preserved {
        body_sources(core.source_coordinates(&path), &selected.entry.atom_uuid)?
    } else {
        let expected = crate::schema::types::field::build_storage_key(
            scope,
            &atom_key_codec::body_key(&selected.entry.atom_uuid),
        );
        super::super::kv::coordinates("main", expected.as_bytes()).to_vec()
    };
    // An existing full value keeps its actual copy, including protein siblings.
    if body.is_empty() {
        return Ok(());
    }
    let mut sources = sources.clone();
    sources.extend(body);
    if !core.replace_sources(&path, &sources) {
        if !preserved {
            core.invalidate_slot(&path);
        }
        return Ok(());
    }
    core.authorize_path(&path);
    Ok(())
}

fn body_sources(
    coordinates: Vec<(String, String)>,
    atom: &str,
) -> StorageResult<Vec<(String, String)>> {
    let mut body = Vec::new();
    for source in coordinates {
        let decoded = crate::storage::laststore::native_routing::decode(&source.1)?;
        if std::str::from_utf8(&decoded)
            .ok()
            .and_then(atom_key_codec::uuid_of)
            == Some(atom)
        {
            body.push(source);
        }
    }
    Ok(body)
}
