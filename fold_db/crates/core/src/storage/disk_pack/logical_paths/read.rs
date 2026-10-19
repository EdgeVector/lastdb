//! Retain full core objects before the field result becomes a response.
use super::*;
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::read::ReadContext;

/// Dirty values belong to the captured core revision, including after a flush.
pub(super) fn captured_lookup(
    runtime: &PackRuntime,
    context: &ReadContext,
    path: &CorePath,
) -> StorageResult<crate::resident::CoreWarmLookup> {
    let selected = context.core_model.lookup(path);
    if !selected.is_unknown() {
        let view = runtime.view.read().expect("captured core path hold");
        let mut core = runtime.core.lock().expect("captured core path retention");
        context.hold_core(&mut core, std::slice::from_ref(path))?;
        if view.revision == context.revision {
            let _ = core.lookup(path);
        }
    }
    Ok(selected)
}

impl PackRuntime {
    pub(super) fn admit_field_path(
        self: &Arc<Self>,
        context: &Arc<ReadContext>,
        path: &LogicalPath,
        facts: source::AtomFacts,
        hint_key: &KeyValue,
    ) -> StorageResult<()> {
        if !field::provenance_current() || transaction::PendingBatch::current(self).is_some() {
            return Ok(());
        }
        let selected = core_read::selected_tip(
            Some(Arc::clone(self)),
            path.scope.as_deref(),
            &path.molecule,
            hint_key,
            &path.atom,
        )?;
        let view = self.view.read().expect("native complete core admission");
        if view.revision != context.revision {
            return Ok(());
        }
        let mut call = crate::read_receipts::lock(&context.call);
        let mut core = self.core.lock().expect("native shared core model");
        let key = path.core();
        let Some(AdmissionTip { tip, sources }) =
            admission_tip(&mut core, &key, &path.atom, selected)
        else {
            return Ok(());
        };
        context.hold_core(&mut core, std::slice::from_ref(&key))?;
        let bytes = body::encoded_len(facts.atom.as_ref())?;
        context.account_memory_record(&mut call, path.id()?.len(), bytes)?;
        let atom_source = (facts.source().0.to_owned(), facts.source().1.to_owned());
        let slot = CoreWarmSlot {
            entry: tip.entry,
            metadata: tip.metadata,
            atom: facts.atom,
            bytes,
        };
        let sequence = (!view.accepted.is_empty()).then_some(view.sequence);
        core.admit(&key, slot, sequence)
            .map_err(|error| StorageError::ReadResourceLimit {
                detail: error.to_string(),
            })?;
        core.clear_sources(&key);
        if !core.link_source(&key, &atom_source.0, &atom_source.1) {
            core.invalidate_slot(&key);
            return Ok(());
        }
        if let Some(sources) = sources {
            for (collection, source) in sources {
                if !core.link_source(&key, &collection, &source) {
                    core.invalidate_slot(&key);
                    return Ok(());
                }
            }
            core.authorize_path(&key);
        }
        Ok(())
    }
}

struct AdmissionTip {
    tip: crate::resident::CoreWarmTip,
    sources: Option<Vec<(String, String)>>,
}

/// Exact core filters select an authorized tip without a disk receipt.
fn admission_tip(
    core: &mut crate::resident::CoreWarmSet,
    path: &CorePath,
    atom: &str,
    selected: Option<core_read::SelectedTip>,
) -> Option<AdmissionTip> {
    if let Some(selected) = selected {
        let sources = selected.sources.or_else(|| {
            retained_tip_sources(core, path, &selected.entry, selected.metadata.as_ref())
        });
        return Some(AdmissionTip {
            tip: crate::resident::CoreWarmTip {
                entry: selected.entry,
                metadata: selected.metadata,
            },
            sources,
        });
    }
    if !core.path_authorized(path) {
        return None;
    }
    let crate::resident::CoreWarmLookup::Tip(tip) = core.lookup(path) else {
        return None;
    };
    (tip.entry.atom_uuid == atom).then(|| AdmissionTip {
        tip,
        sources: Some(core.source_coordinates(path)),
    })
}

/// Promotion keeps the already certified tip under the same publication lock.
fn retained_tip_sources(
    core: &mut crate::resident::CoreWarmSet,
    path: &CorePath,
    entry: &AtomEntry,
    metadata: Option<&KeyMetadata>,
) -> Option<Vec<(String, String)>> {
    if !core.path_authorized(path) {
        return None;
    }
    let selected = core.lookup(path).into_tip()?;
    (selected.entry == *entry && selected.metadata.as_ref() == metadata)
        .then(|| core.source_coordinates(path))
}

pub(super) fn refresh_tip(
    core: &mut crate::resident::CoreWarmSet,
    path: &CorePath,
    tip: core_read::SelectedTip,
    primary: &str,
) {
    let Some(sources) = tip.sources else { return };
    if !core.refresh_tip(path, tip.entry, tip.metadata) {
        return;
    }
    let previous = core.source_coordinates(path);
    let body = crate::storage::disk_pack::kv::coordinates("main", primary.as_bytes())
        .into_iter()
        .filter(|source| previous.contains(source));
    core.clear_sources(path);
    if body
        .chain(sources)
        .all(|(collection, key)| core.link_source(path, &collection, &key))
    {
        core.authorize_path(path);
    } else {
        core.invalidate_slot(path);
    }
}
