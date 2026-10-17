use super::*;

/// Called after the typed reader accepts a header; opaque bytes never qualify.
pub(crate) fn note_molecule_header(key: &str, version: u64) {
    let _ = READ.try_with(|context| {
        if context.write_owner.is_some() {
            return;
        }
        let collection =
            super::super::super::laststore::native_routing::write_collection(key.as_bytes());
        let id = super::super::super::laststore::native_routing::encode(key.as_bytes());
        context
            .dependencies
            .lock()
            .expect("native read dependencies")
            .header(collection.to_string(), id, version);
    });
}

impl ReadContext {
    pub(in crate::storage::disk_pack) fn refresh_molecule_version(
        &self,
        witness: Option<u64>,
        value: &mut crate::schema::types::field::FieldValue,
    ) -> bool {
        if value.molecule_version.is_none() {
            return true;
        }
        let (Some(token), Some(molecule)) = (witness, value.molecule_uuid.as_deref()) else {
            return true;
        };
        let Some(proof) = self.validated(token) else {
            return false;
        };
        match proof.molecule_version(molecule) {
            Ok(Some(version)) => value.molecule_version = Some(version),
            Ok(None) => {}
            Err(()) => return false,
        }
        true
    }
}
