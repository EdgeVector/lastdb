use super::*;

const MAX_HISTORY_KEYS: usize = 10_000;
const MAX_HISTORY_BYTES: usize = 4 * 1024 * 1024;
const MAX_HISTORY_BATCHES: usize = 1024;

impl Validity {
    /// The read-view lock makes this change visible atomically with acceptance.
    pub(in crate::storage::disk_pack) fn record(
        &mut self,
        revision: u64,
        coordinates: Vec<ChangedCoordinate>,
    ) {
        let coordinates: Vec<_> = coordinates
            .into_iter()
            .map(|change| ((change.collection.clone(), change.key.clone()), change))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect();
        let bytes = coordinates.iter().fold(128usize, |total, change| {
            total
                .saturating_add(192)
                .saturating_add(change.collection.len())
                .saturating_add(change.key.len())
        });
        if coordinates.len() > MAX_HISTORY_KEYS || bytes > MAX_HISTORY_BYTES {
            self.invalidate(revision);
            return;
        }
        self.history_keys += coordinates.len();
        self.history_bytes += bytes;
        self.history.push_back(Changes {
            revision,
            coordinates,
            bytes,
        });
        while self.history_keys > MAX_HISTORY_KEYS
            || self.history_bytes > MAX_HISTORY_BYTES
            || self.history.len() > MAX_HISTORY_BATCHES
        {
            let old = self
                .history
                .pop_front()
                .expect("bounded native change history");
            self.floor = self.floor.max(old.revision);
            self.history_keys -= old.coordinates.len();
            self.history_bytes -= old.bytes;
        }
    }

    pub(in crate::storage::disk_pack) fn invalidate(&mut self, revision: u64) {
        self.floor = self.floor.max(revision);
        self.history.clear();
        self.history_keys = 0;
        self.history_bytes = 0;
    }

    pub(in crate::storage::disk_pack) fn valid(&mut self, token: u64, revision: u64) -> bool {
        let Some(witness) = self.witnesses.get(&token) else {
            return false;
        };
        if witness.revision < self.floor || witness.revision > revision {
            return false;
        }
        if let Some((_, valid)) = witness.verified.filter(|(checked, _)| *checked == revision) {
            return valid;
        }
        let valid = self
            .history
            .iter()
            .filter(|changes| changes.revision > witness.revision && changes.revision <= revision)
            .all(|changes| {
                changes
                    .coordinates
                    .iter()
                    .all(|change| !witness.reads.intersects(change))
            });
        self.witnesses
            .get_mut(&token)
            .expect("held dependency witness")
            .verified = Some((revision, valid));
        valid
    }
}
