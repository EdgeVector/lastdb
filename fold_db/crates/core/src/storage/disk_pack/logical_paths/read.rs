//! Retain full core objects before the field result becomes a response.
use super::*;
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::read::ReadContext;

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
        let Some(mut held) = super::lock_current_receipt(
            self,
            context,
            "native complete core admission",
            "native shared core model",
        ) else {
            return Ok(());
        };
        let key = path.core();
        let Some(AdmissionTip { tip, sources }) =
            admission_tip(&mut held.core, &key, &path.atom, selected)
        else {
            return Ok(());
        };
        context.hold_core(&mut held.core, std::slice::from_ref(&key))?;
        let bytes = body::encoded_len(facts.atom.as_ref())?;
        context.account_memory_record(&mut held.call, path.id()?.len(), bytes)?;
        let atom_source = (facts.source().0.to_owned(), facts.source().1.to_owned());
        let slot = CoreWarmSlot {
            entry: tip.entry,
            metadata: tip.metadata,
            atom: facts.atom,
            bytes,
        };
        held.core
            .admit(&key, slot)
            .map_err(|error| StorageError::ReadResourceLimit {
                detail: error.to_string(),
            })?;
        held.core.clear_sources(&key);
        if !held.core.link_source(&key, &atom_source.0, &atom_source.1) {
            held.core.invalidate_slot(&key);
            return Ok(());
        }
        if let Some(sources) = sources {
            for (collection, source) in sources {
                if !held.core.link_source(&key, &collection, &source) {
                    held.core.invalidate_slot(&key);
                    return Ok(());
                }
            }
            held.core.authorize_path(&key);
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
    let body = crate::storage::disk_pack::kv::coordinate("main", primary.as_bytes());
    let body = previous.contains(&body).then_some(body);
    core.clear_sources(path);
    if body
        .into_iter()
        .chain(sources)
        .all(|(collection, key)| core.link_source(path, &collection, &key))
    {
        core.authorize_path(path);
    } else {
        core.invalidate_slot(path);
    }
}
