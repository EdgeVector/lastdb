//! Dependency witnesses refer only to this call's immutable accepted revision.

use super::*;
use crate::storage::disk_pack::logical_paths::body::ReadVersion;

impl ReadContext {
    pub(in crate::storage::disk_pack) fn note_points(
        &self,
        coordinates: &[(String, String)],
        values: &[Option<Vec<u8>>],
    ) {
        if self.write_owner.is_none() {
            self.dependencies
                .lock()
                .expect("native read dependencies")
                .points(coordinates, values);
        }
    }

    pub(in crate::storage::disk_pack) fn note_rows(
        &self,
        collection: &str,
        rows: &[(String, Vec<u8>)],
    ) {
        if self.write_owner.is_none() {
            self.dependencies
                .lock()
                .expect("native read dependencies")
                .rows(collection, rows);
        }
    }

    pub(in crate::storage::disk_pack) fn note_range(
        &self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) {
        if self.write_owner.is_none() {
            self.dependencies
                .lock()
                .expect("native read dependencies")
                .range(collection, start, end);
        }
    }

    pub(in crate::storage::disk_pack) fn witness(&self) -> Option<u64> {
        if self.write_owner.is_some() {
            return None;
        }
        let mut dependencies = self.dependencies.lock().expect("native read dependencies");
        self.validity
            .lock()
            .expect("native dependency validity")
            .witness(&mut dependencies, self.version.revision)
    }

    pub(in crate::storage::disk_pack) fn provenance_valid(
        &self,
        version: ReadVersion,
        witness: Option<u64>,
    ) -> bool {
        self.write_owner.is_none()
            && match witness {
                Some(token) => self.validated(token).is_some(),
                None => version == self.version,
            }
    }

    pub(in crate::storage::disk_pack) fn inherit_witness(&self, witness: Option<u64>) -> bool {
        let proof = match witness {
            Some(token) => match self.validated(token) {
                Some(proof) => Some((token, proof)),
                None => return false,
            },
            None => None,
        };
        let mut dependencies = self.dependencies.lock().expect("native read dependencies");
        match proof {
            Some((token, proof)) => proof.inherit(token, &mut dependencies),
            None => dependencies.disable(),
        }
        true
    }
}
