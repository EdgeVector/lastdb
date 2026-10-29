//! Exact source adapters borrow existing warm tips and atoms under the core lock.

use super::*;

pub(crate) enum CoreSourceRecord<'a> {
    Atom(&'a Atom),
    Tip(&'a AtomEntry, Option<&'a KeyMetadata>),
}

impl CoreWarmSet {
    pub(crate) fn tip_record_source(&self, path: &CorePath) -> Option<(String, String)> {
        let source = self.slots.get(self.slot_of(path)?)?.tip_source?;
        Some(self.sources.coordinates(source))
    }

    /// Only an observed matching record can give one source raw-tip authority.
    pub(crate) fn authorize_tip_source(
        &mut self,
        collection: &str,
        key: &str,
        entry: &AtomEntry,
        metadata: Option<&KeyMetadata>,
    ) {
        let Some(source) = self.sources.find(collection, key) else {
            return;
        };
        let ids: Vec<_> = self
            .sources
            .get(source)
            .expect("core source")
            .slots
            .iter()
            .collect();
        for id in ids {
            let slot = self.slots.get_mut(id).expect("source-linked slot");
            if slot.tip.as_ref() == Some(entry) && slot.metadata.as_deref() == metadata {
                slot.tip_source = Some(source);
            }
        }
    }

    /// Exact coordinates bind the scope. A UUID alone never authorizes reuse.
    /// Multiple inconsistent copies refuse reuse and leave the answer to disk.
    pub(crate) fn source_record(
        &mut self,
        collection: &str,
        key: &str,
        body_uuid: Option<&str>,
    ) -> Option<CoreSourceRecord<'_>> {
        let source = self.sources.find(collection, key)?;
        let mut selected = None;
        for id in self.sources.get(source)?.slots.iter() {
            let slot = self.slots.get(id)?;
            let eligible = match body_uuid {
                Some(uuid) => slot
                    .atom
                    .as_ref()
                    .is_some_and(|atom| atom.value.uuid() == uuid),
                None => slot.tip_source == Some(source) && slot.tip.is_some(),
            };
            if !eligible {
                continue;
            }
            if let Some(previous) = selected {
                let previous = self.slots.get(previous)?;
                let identical = match body_uuid {
                    Some(_) => {
                        previous.atom.as_ref()?.value.as_ref() == slot.atom.as_ref()?.value.as_ref()
                    }
                    None => previous.tip == slot.tip && previous.metadata == slot.metadata,
                };
                if !identical {
                    return None;
                }
            }
            selected = Some(id);
        }
        let id = selected?;
        let path = *self.slots.get(id)?.owners.first()?;
        self.touch_id(path);
        self.hits = self.hits.saturating_add(1);
        let slot = self.slots.get(id)?;
        Some(match body_uuid {
            Some(_) => CoreSourceRecord::Atom(slot.atom.as_ref()?.value.as_ref()),
            None => CoreSourceRecord::Tip(slot.tip.as_ref()?, slot.metadata.as_deref()),
        })
    }
}
