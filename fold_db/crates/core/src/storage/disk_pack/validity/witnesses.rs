use super::*;

const MAX_WITNESSES: usize = 10_000;
const MAX_WITNESS_BYTES: usize = 16 * 1024 * 1024;

impl Validity {
    pub(in crate::storage::disk_pack) fn witness(
        &mut self,
        dependencies: &mut Dependencies,
        revision: u64,
    ) -> Option<u64> {
        if dependencies.overflow || revision < self.floor {
            return None;
        }
        if let Some(token) = dependencies
            .cached
            .filter(|token| self.witnesses.contains_key(token))
        {
            return Some(token);
        }
        let token = self.next.checked_add(1)?;
        self.next = token;
        let reads = Arc::new(dependencies.reads.clone());
        self.witness_bytes = self.witness_bytes.saturating_add(reads.bytes + 128);
        self.witnesses.insert(
            token,
            Witness {
                revision,
                reads,
                verified: None,
            },
        );
        while self.witnesses.len() > MAX_WITNESSES || self.witness_bytes > MAX_WITNESS_BYTES {
            let (_, old) = self
                .witnesses
                .pop_first()
                .expect("bounded native witnesses");
            self.witness_bytes -= old.reads.bytes + 128;
        }
        dependencies.cached = Some(token);
        Some(token)
    }

    /// Warm hits contribute their original dependencies to the enclosing call.
    pub(in crate::storage::disk_pack) fn inherit(
        &self,
        token: u64,
        dependencies: &mut Dependencies,
        revision: u64,
    ) {
        if dependencies.overflow || !dependencies.inherited.insert(token) {
            return;
        }
        if dependencies.inherited.len() > MAX_WITNESSES {
            dependencies.overflow = true;
            return;
        }
        let Some(witness) = self.witnesses.get(&token) else {
            dependencies.overflow = true;
            return;
        };
        for ((collection, key), digest) in &witness.reads.points {
            let coordinate = (collection.clone(), key.clone());
            if dependencies.reads.points.contains_key(&coordinate) {
                continue;
            }
            if dependencies.room(
                192usize
                    .saturating_add(collection.len())
                    .saturating_add(key.len()),
            ) {
                dependencies.reads.points.insert(coordinate, digest.clone());
            }
        }
        for (coordinate, initial) in &witness.reads.headers {
            let version = self.header_at(witness, coordinate, revision, *initial);
            dependencies.header(coordinate.0.clone(), coordinate.1.clone(), version);
        }
        for range in &witness.reads.ranges {
            dependencies.range(&range.collection, &range.start, &range.end);
        }
    }
}
