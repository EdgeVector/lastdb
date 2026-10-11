//! A share includes reachable file rows one at a time under its existing admission.

use super::*;
use crate::db_operations::DbOperations;
use crate::sync::cloud_purge::memory::{self, Admission};
use crate::sync::snapshot::{NamespaceData, Snapshot, SnapshotEntry};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use std::collections::BTreeSet;

pub(super) async fn add_share_file_closure(
    ops: &DbOperations,
    snapshot: &mut Snapshot,
    admission: &Admission,
) -> SyncResult<()> {
    let keys = atom_keys(snapshot)?;
    let sizes = memory::raw_lengths(
        admission,
        memory::snapshot_peak(snapshot.cloud_purge_share_bytes()?, 0)?,
        ops.namespaced_store().as_ref(),
        "main",
        &keys,
    )?;
    let mut refs = BTreeSet::new();
    for (key, size) in keys.iter().zip(sizes) {
        admission.check(memory::snapshot_peak(
            snapshot.cloud_purge_share_bytes()?,
            memory::read_peak(size.ok_or_else(|| error("share source atom disappeared"))?)?,
        )?)?;
        for atom in ops
            .atoms()
            .get_atoms_by_storage_keys(std::slice::from_ref(key))
            .await
            .map_err(|_| error("share source atom decode failed"))?
        {
            let atom = atom.ok_or_else(|| error("share source atom absent"))?;
            refs.extend(crate::atom::file_pointer::blob_refs_of_atom(
                atom.content(),
                atom.metadata(),
            ));
        }
    }
    let ids: Vec<_> = refs.into_iter().collect();
    let sizes = memory::raw_lengths(
        admission,
        memory::snapshot_peak(snapshot.cloud_purge_share_bytes()?, 0)?,
        ops.namespaced_store().as_ref(),
        "cas_blobs",
        &ids,
    )?;
    let cas = ops.open_namespace("cas_blobs").await?;
    let mut entries = Vec::new();
    let mut bound = snapshot.cloud_purge_share_bytes()?;
    for (id, size) in ids.iter().zip(sizes) {
        let size = size.ok_or_else(|| error("owned share source lacks exact file bytes"))?;
        bound = memory::checked_sum(&[
            bound,
            memory::checked_scale(memory::outer_bound(size), 2)?,
            id.len() as u64 * 2,
            256,
        ])?;
        admission.check(memory::snapshot_peak(bound, memory::read_peak(size)?)?)?;
        let value = cas
            .get(id.as_bytes())
            .await?
            .ok_or_else(|| error("share file disappeared after admission"))?;
        entries.push(SnapshotEntry {
            key: BASE64.encode(id),
            value: BASE64.encode(value),
        });
    }
    snapshot
        .namespaces
        .retain(|namespace| namespace.name != "cas_blobs");
    snapshot.namespaces.push(NamespaceData {
        name: "cas_blobs".into(),
        entries,
    });
    Ok(())
}

fn atom_keys(snapshot: &Snapshot) -> SyncResult<Vec<String>> {
    let mut keys = Vec::new();
    for namespace in &snapshot.namespaces {
        if namespace.name != "main" && namespace.name != "atoms" {
            continue;
        }
        for entry in &namespace.entries {
            let key = String::from_utf8(
                BASE64
                    .decode(&entry.key)
                    .map_err(|_| error("invalid share key"))?,
            )
            .map_err(|_| error("invalid share key"))?;
            if crate::atom::atom_key_codec::uuid_of(&key).is_some() {
                keys.push(key);
            }
        }
    }
    Ok(keys)
}

fn error(message: &str) -> SyncError {
    SyncError::Storage(message.into())
}
