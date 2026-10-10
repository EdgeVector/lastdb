//! Record, schema, field and protein admission for the logical resident set.

use super::*;

impl LogicalResidentSet {
    /// Live molecule-tip count. PR 7 reads this gauge.
    pub fn resident_key_count(&self) -> usize {
        self.tips.len()
    }

    /// Used logical records: each fetched schema, field, tip, tombstone, and atom.
    ///
    /// Decision `decision-2026-10-02-warm-set-exact-logical-key-budget`.
    /// A protein is not in this count. A tombstone counts. Before the flush
    /// it cannot leave. After the flush, a tombstone with no hold can leave
    /// when it is the least recently used record past the budget.
    pub fn logical_record_count(&self) -> usize {
        self.tips.len()
            + self.tombstones.len()
            + self.atom_bodies.len()
            + self.schemas.len()
            + self.fields.len()
            + self.records.len()
    }

    /// A fetched record for exactly `(collection, id)`, and mark it most
    /// recent. `Some(None)` means a read found the id absent and no write
    /// to it followed. `None` means the set does not know; load it.
    pub fn fetched_record(&mut self, collection: &str, id: &str) -> Option<Option<Vec<u8>>> {
        let key = RecordKey {
            collection: collection.to_string(),
            id: id.to_string(),
        };
        let body = self.records.get(&key)?.clone();
        self.touch(ResidentKey::Record {
            collection: key.collection,
            id: key.id,
        });
        Some(body)
    }

    /// True when the set holds a fetched record (present or absent) for the id.
    pub fn has_fetched_record(&self, collection: &str, id: &str) -> Option<bool> {
        self.records
            .get(&RecordKey {
                collection: collection.to_string(),
                id: id.to_string(),
            })
            .map(Option::is_some)
    }

    /// Write epoch for `id`. Take it under the set lock before a load and
    /// pass it to [`Self::admit_record`] after the load.
    pub fn record_epoch(&self, id: &str) -> u64 {
        self.record_epochs[record_stripe(id)]
    }

    /// Keep what one load of `(collection, id)` returned: the stored bytes,
    /// or `None` when the id was absent. A write to that id after `epoch`
    /// was taken refuses the admit, so stale bytes do not stay warm.
    pub fn admit_record(
        &mut self,
        collection: &str,
        id: &str,
        body: Option<Vec<u8>>,
        epoch: u64,
    ) -> bool {
        if self.record_epoch(id) != epoch {
            return false;
        }
        let key = RecordKey {
            collection: collection.to_string(),
            id: id.to_string(),
        };
        self.records.insert(key.clone(), body);
        self.touch(ResidentKey::Record {
            collection: key.collection,
            id: key.id,
        });
        self.purge_over_cap();
        true
    }

    /// Drop the fetched record for `(collection, id)`. Call after the store
    /// write to that id lands, so a concurrent load cannot admit older bytes.
    pub fn forget_record(&mut self, collection: &str, id: &str) {
        let stripe = record_stripe(id);
        self.record_epochs[stripe] = self.record_epochs[stripe].wrapping_add(1);
        self.negative_barriers.remove(collection, id);
        let key = RecordKey {
            collection: collection.to_string(),
            id: id.to_string(),
        };
        if self.records.remove(&key).is_some() {
            self.forget_key(&ResidentKey::Record {
                collection: key.collection,
                id: key.id,
            });
            self.publish_occupancy();
        }
    }

    /// Drop every fetched record. An admin rewrite that writes the store
    /// directly (restore, residue drain) calls this after its write.
    pub fn forget_all_records(&mut self) {
        for epoch in &mut self.record_epochs {
            *epoch = epoch.wrapping_add(1);
        }
        self.negative_barriers.clear();
        let keys: Vec<RecordKey> = self.records.drain().map(|(key, _)| key).collect();
        for key in keys {
            self.forget_key(&ResidentKey::Record {
                collection: key.collection,
                id: key.id,
            });
        }
        self.publish_occupancy();
    }

    /// Recency tick of a used record. Higher is more recent.
    pub fn recency(&self, key: &ResidentKey) -> Option<u64> {
        self.recency.get(key).copied()
    }

    /// Mark `key` as the most recent used record.
    pub fn touch(&mut self, key: ResidentKey) {
        if let Some(previous) = self.recency.remove(&key) {
            self.order.remove(&previous);
        }
        self.clock = self.clock.saturating_add(1);
        self.order.insert(self.clock, key.clone());
        self.recency.insert(key, self.clock);
    }

    pub fn schema(&self, name: &str) -> Option<&Schema> {
        self.schemas.get(name).map(|entry| &entry.value)
    }

    pub fn field(&self, schema: &str, field: &str) -> Option<&FieldEntry> {
        self.fields
            .get(&field_key(schema, field))
            .map(|entry| &entry.value)
    }

    pub fn protein(&self, protein_id: &str) -> Option<&[MoleculeId]> {
        self.proteins
            .get(protein_id)
            .map(|entry| entry.value.as_slice())
    }
}
