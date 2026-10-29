//! Exact source adapters borrow existing warm tips and atoms under the core lock.

use super::*;

#[derive(Clone, Copy)]
pub(crate) enum CoreSourceRecord<'a> {
    Atom(&'a Arc<Atom>, usize),
    Tip(&'a AtomEntry, Option<&'a KeyMetadata>, usize),
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
        // A generation winner also carries shadow state. Its raw value is not
        // a PerKeyRecord, so the logical tip cannot answer that stored record.
        if collection != "tips" || crate::kind_partition::rest_of(key, "mk").is_none() {
            return;
        }
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
        if body_uuid.is_none() {
            let slot = self.slots.get_mut(id)?;
            if slot.tip_wire_bytes.is_none() {
                let record = TipRecord {
                    entry: slot.tip.as_ref()?,
                    meta: slot.metadata.as_deref(),
                };
                let mut bytes = RecordLength(0);
                serde_json::to_writer(&mut bytes, &record).ok()?;
                slot.tip_wire_bytes = Some(bytes.0);
            }
        }
        let slot = self.slots.get(id)?;
        Some(match body_uuid {
            Some(_) => {
                CoreSourceRecord::Atom(&slot.atom.as_ref()?.value, slot.atom.as_ref()?.bytes)
            }
            None => CoreSourceRecord::Tip(
                slot.tip.as_ref()?,
                slot.metadata.as_deref(),
                slot.tip_wire_bytes?,
            ),
        })
    }
}

#[derive(serde::Serialize)]
struct TipRecord<'a> {
    entry: &'a AtomEntry,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<&'a KeyMetadata>,
}

struct RecordLength(usize);
impl std::io::Write for RecordLength {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("tip size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
