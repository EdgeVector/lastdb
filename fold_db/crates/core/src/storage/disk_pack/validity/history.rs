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
            tracing::debug!(target: "lastdb::native_warm_validity", token, revision, "dependency witness left the bounded registry");
            return false;
        };
        if witness.revision < self.floor || witness.revision > revision {
            tracing::debug!(target: "lastdb::native_warm_validity", token, revision, saved = witness.revision, floor = self.floor, "dependency witness is outside retained history");
            return false;
        }
        if let Some((_, valid)) = witness.verified.filter(|(checked, _)| *checked == revision) {
            return valid;
        }
        let changed = self
            .history
            .iter()
            .filter(|changes| changes.revision > witness.revision && changes.revision <= revision)
            .find_map(|changes| {
                changes
                    .coordinates
                    .iter()
                    .find(|change| witness.reads.intersects(change))
            });
        let valid = changed.is_none();
        if let Some(change) = changed {
            tracing::debug!(target: "lastdb::native_warm_validity", token, revision, saved = witness.revision,
                collection = %change.collection, key_digest = %crate::hex::sha256_hex(change.key.as_bytes()),
                molecule_header_key = change.key.starts_with("mh:"),
                home_conflict_key = change.key.starts_with("hcu:"),
                header = change.header.is_some(), header_dependencies = witness.reads.headers.len(),
                "a captured dependency changed");
        }
        self.witnesses
            .get_mut(&token)
            .expect("held dependency witness")
            .verified = Some((revision, valid));
        valid
    }
}
