//! A successful check freezes only dependency metadata and header scalars.

use super::*;

pub(in crate::storage::disk_pack) struct ValidatedWitness {
    reads: Arc<DependencySet>,
    headers: BTreeMap<(String, String), u64>,
}

impl Validity {
    pub(in crate::storage::disk_pack) fn validate(
        &mut self,
        token: u64,
        revision: u64,
    ) -> Option<Arc<ValidatedWitness>> {
        if !self.valid(token, revision) {
            return None;
        }
        let witness = self.witnesses.get(&token)?;
        let headers = witness
            .reads
            .headers
            .iter()
            .map(|(coordinate, initial)| {
                (
                    coordinate.clone(),
                    self.header_at(witness, coordinate, revision, *initial),
                )
            })
            .collect();
        Some(Arc::new(ValidatedWitness {
            reads: Arc::clone(&witness.reads),
            headers,
        }))
    }
}

impl ValidatedWitness {
    pub(in crate::storage::disk_pack) fn bytes(&self) -> usize {
        self.headers.keys().fold(
            self.reads.bytes.saturating_add(128),
            |bytes, (collection, key)| {
                bytes
                    .saturating_add(160)
                    .saturating_add(collection.len())
                    .saturating_add(key.len())
            },
        )
    }

    pub(in crate::storage::disk_pack) fn molecule_version(
        &self,
        molecule: &str,
    ) -> Result<Option<u64>, ()> {
        let suffix = format!("mh:{molecule}");
        let mut matching = self
            .headers
            .iter()
            .filter(|((_, key), _)| key.ends_with(&suffix));
        let Some((_, version)) = matching.next() else {
            return Ok(None);
        };
        if matching.next().is_some() {
            return Err(());
        }
        Ok(Some(*version))
    }

    pub(in crate::storage::disk_pack) fn inherit(
        &self,
        token: u64,
        dependencies: &mut Dependencies,
    ) {
        if dependencies.overflow || !dependencies.inherited.insert(token) {
            return;
        }
        if dependencies.inherited.len() > MAX_DEPENDENCIES {
            dependencies.disable();
            return;
        }
        for ((collection, key), digest) in &self.reads.points {
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
        for (coordinate, version) in &self.headers {
            dependencies.header(coordinate.0.clone(), coordinate.1.clone(), *version);
        }
        for range in &self.reads.ranges {
            dependencies.range(&range.collection, &range.start, &range.end);
        }
    }
}
