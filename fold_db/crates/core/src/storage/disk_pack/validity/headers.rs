//! A header witness retains only the scalar returned as molecule provenance.

use super::*;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::storage::disk_pack) struct ChangedCoordinate {
    pub collection: String,
    pub key: String,
    pub digest: Option<String>,
    pub header: Option<u64>,
}

pub(in crate::storage::disk_pack) fn header_version(key: &str, value: &[u8]) -> Option<u64> {
    let (prefix, molecule) = key.rsplit_once("mh:")?;
    if !prefix.is_empty() && !prefix.ends_with(':') {
        return None;
    }
    crate::atom::molecule_uuid::parse_molecule_uuid_bytes(molecule)?;
    serde_json::from_slice::<crate::db_operations::atom_store::MoleculeHeader>(value)
        .ok()
        .map(|header| header.version)
}

pub(in crate::storage::disk_pack) fn changed(
    collection: String,
    key: String,
    body: Option<&[u8]>,
) -> ChangedCoordinate {
    ChangedCoordinate {
        header: body.and_then(|body| header_version(&key, body)),
        digest: body.map(crate::hex::sha256_hex),
        collection,
        key,
    }
}

impl Dependencies {
    pub(in crate::storage::disk_pack) fn header(
        &mut self,
        collection: String,
        key: String,
        version: u64,
    ) {
        if self.overflow {
            return;
        }
        let coordinate = (collection, key);
        if !self.reads.points.contains_key(&coordinate) {
            self.disable();
            return;
        }
        if self.reads.headers.get(&coordinate) == Some(&version) {
            return;
        }
        if self.room(
            160usize
                .saturating_add(coordinate.0.len())
                .saturating_add(coordinate.1.len()),
        ) {
            self.reads.headers.insert(coordinate, version);
        }
    }
}

impl Validity {
    pub(super) fn header_at(
        &self,
        witness: &Witness,
        coordinate: &(String, String),
        revision: u64,
        initial: u64,
    ) -> u64 {
        self.history
            .iter()
            .filter(|changes| changes.revision > witness.revision && changes.revision <= revision)
            .flat_map(|changes| &changes.coordinates)
            .filter(|change| (&change.collection, &change.key) == (&coordinate.0, &coordinate.1))
            .filter_map(|change| change.header)
            .next_back()
            .unwrap_or(initial)
    }

    pub(in crate::storage::disk_pack) fn molecule_version(
        &self,
        token: u64,
        revision: u64,
        molecule: &str,
    ) -> Result<Option<u64>, ()> {
        let witness = self.witnesses.get(&token).ok_or(())?;
        if witness.revision < self.floor || witness.revision > revision {
            return Err(());
        }
        let suffix = format!("mh:{molecule}");
        let mut matching = witness
            .reads
            .headers
            .iter()
            .filter(|((_, key), _)| key.ends_with(&suffix));
        let Some((coordinate, version)) = matching.next() else {
            return Ok(None);
        };
        if matching.next().is_some() {
            return Err(());
        }
        Ok(Some(
            self.header_at(witness, coordinate, revision, *version),
        ))
    }
}
