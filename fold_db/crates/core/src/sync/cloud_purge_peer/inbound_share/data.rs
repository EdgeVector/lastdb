//! Admit exact inbound atom bodies and stopped receiver replacement writes.

use super::*;
use crate::sync::cloud_purge::memory::{self, Admission};

pub(super) async fn blob_refs(
    previous: &DbOperations,
    keys: &[String],
    admission: &Admission,
) -> SyncResult<BTreeSet<String>> {
    let sizes = memory::raw_lengths(
        admission,
        0,
        previous.namespaced_store().as_ref(),
        "main",
        keys,
    )?;
    let mut refs = BTreeSet::new();
    for (key, size) in keys.iter().zip(sizes) {
        let size = size.ok_or_else(|| error("an inbound atom size is absent"))?;
        admission.check(memory::read_peak(size)?)?;
        let atom = previous
            .atoms()
            .get_atoms_by_storage_keys(std::slice::from_ref(key))
            .await
            .map_err(error)?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| error("an inbound atom is absent"))?;
        refs.extend(crate::atom::file_pointer::blob_refs_of_atom(
            atom.content(),
            atom.metadata(),
        ));
    }
    Ok(refs)
}

pub(super) async fn replace_rows(
    candidate: &DbOperations,
    rows: &[(Vec<u8>, Vec<u8>)],
    admission: &Admission,
    digest: &mut sha2::Sha256,
) -> SyncResult<()> {
    let store = candidate.namespaced_store();
    let raw = store
        .raw_last_store()
        .ok_or_else(|| error("inbound preservation requires a LastStore candidate"))?;
    let destination = candidate.open_namespace("main").await.map_err(error)?;
    // The inactive store still needs its old values for deletion inverses.
    let inherited: Vec<_> = source::physical_keys(&raw, "from:", admission)?
        .into_iter()
        .collect();
    let sizes = memory::raw_lengths(admission, 0, store.as_ref(), "main", &inherited)?;
    for (keys, sizes) in inherited.chunks(128).zip(sizes.chunks(128)) {
        let peak = sizes.iter().flatten().try_fold(0, |sum, size| {
            memory::checked_sum(&[sum, memory::body_peak(*size)?])
        })?;
        admission.check(peak)?;
        destination
            .batch_delete(keys.iter().map(|key| key.as_bytes().to_vec()).collect())
            .await
            .map_err(error)?;
    }
    for page in rows.chunks(128) {
        let bound = page.iter().try_fold(0, |sum, (key, value)| {
            memory::checked_sum(&[sum, key.len() as u64, value.len() as u64, 512])
        })?;
        admission.check(memory::checked_scale(bound, 16)?)?;
        hash(digest, "main", page);
        destination
            .restore_batch_put(page.to_vec())
            .await
            .map_err(error)?;
    }
    Ok(())
}
