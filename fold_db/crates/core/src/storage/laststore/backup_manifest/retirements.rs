//! Pending purged-atom retirement sidecar, CAS-proven unbackable retirement and
//! named-hole exclusion applied while cutting a manifest.

use super::*;

pub(super) fn pending_purged_atom_retirements_path(
    store: &LastStoreNamespacedStore,
) -> StorageResult<PathBuf> {
    store
        .durable_sidecar_dir()
        .map(|dir| dir.join(PENDING_PURGED_ATOM_RETIREMENTS_FILE))
        .ok_or_else(|| {
            StorageError::BackendError(
                "atom retirement requires a durable high-water sidecar".to_string(),
            )
        })
}

pub(in super::super) fn load_pending_purged_atom_retirements(
    store: &LastStoreNamespacedStore,
) -> StorageResult<PendingPurgedAtomRetirements> {
    let path = pending_purged_atom_retirements_path(store)?;
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
            StorageError::BackendError(format!(
                "decode pending purged atom retirements {}: {e}",
                path.display()
            ))
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(PendingPurgedAtomRetirements::default())
        }
        Err(e) => Err(StorageError::IoError(e)),
    }
}

pub(in super::super) fn write_pending_purged_atom_retirements(
    store: &LastStoreNamespacedStore,
    state: &PendingPurgedAtomRetirements,
) -> StorageResult<()> {
    let path = pending_purged_atom_retirements_path(store)?;
    let parent = path.parent().ok_or_else(|| {
        StorageError::BackendError("pending atom retirement path has no parent".to_string())
    })?;
    fs::create_dir_all(parent).map_err(StorageError::IoError)?;
    let bytes = serde_json::to_vec_pretty(state).map_err(|e| {
        StorageError::BackendError(format!("encode pending purged atom retirements: {e}"))
    })?;
    let tmp = path.with_file_name(format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(PENDING_PURGED_ATOM_RETIREMENTS_FILE),
        std::process::id()
    ));
    let _ = fs::remove_file(&tmp);
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp).map_err(StorageError::IoError)?;
    file.write_all(&bytes).map_err(StorageError::IoError)?;
    file.sync_all().map_err(StorageError::IoError)?;
    fs::rename(&tmp, &path).map_err(StorageError::IoError)?;
    fs::File::open(parent)
        .and_then(|dir| dir.sync_all())
        .map_err(StorageError::IoError)?;
    Ok(())
}

/// Split atom chunks into (kept, retired_shas): retire only those absent from
/// the local walk **and** absent from a complete cloud listing.
pub(super) fn partition_unbackable_carried_forward_atoms(
    atom_chunks: &[BackupChunkRef],
    local_atom_keys: &BTreeSet<(String, u16, Option<u32>, String)>,
    cloud_present_shas: &BTreeSet<String>,
) -> (Vec<BackupChunkRef>, Vec<String>) {
    let mut kept = Vec::with_capacity(atom_chunks.len());
    let mut retired_shas = Vec::new();
    for chunk in atom_chunks {
        let key = chunk_key(chunk);
        let on_disk = local_atom_keys.contains(&key);
        if on_disk || cloud_present_shas.contains(chunk.object_sha256()) {
            kept.push(chunk.clone());
        } else {
            // Carried-forward, missing locally, missing in cloud → unusable.
            retired_shas.push(chunk.sha256.clone());
        }
    }
    retired_shas.sort();
    retired_shas.dedup();
    (dedupe_and_sort_chunks(kept), retired_shas)
}

/// Count DISTINCT digests named by `manifest` that are not upload candidates
/// (the store walk). Carried-forward atom refs that reseal removed from disk
/// show up here; a post-reconcile cut with a complete cloud listing should
/// trend this to 0 for digests also gone from the object store.
///
/// Distinct, not per-ref: two collections can seal identical bytes, and the
/// bucket holds one object per digest. This number is read against the CAS
/// shortfall ("N chunks not yet in cloud"), which is also per-object, so
/// counting refs would overstate the repair.
pub fn unbackable_manifest_chunk_count(
    manifest: &BackupManifest,
    candidate_shas: &BTreeSet<String>,
) -> usize {
    manifest
        .atom_chunks
        .iter()
        .chain(manifest.mutable_chunks.iter())
        .filter(|chunk| !candidate_shas.contains(chunk.object_sha256()))
        .map(BackupChunkRef::object_sha256)
        .collect::<BTreeSet<&str>>()
        .len()
}

/// Digests that are both (a) named by the held cut's atom list and (b) absent
/// from the local candidate set and (c) proven missing by CAS HEAD after a
/// full reseed — i.e. unusable for upload *or* restore.
///
/// Intersection of CAS shortfall with the unbackable set. Listing-only
/// presence can false-positive; HEAD is the same truth CAS uses to publish,
/// so retiring these digests does not drop a still-restorable cloud object.
#[must_use]
pub fn cas_proven_unbackable_atom_shas(
    manifest: &BackupManifest,
    candidate_shas: &BTreeSet<String>,
    cas_missing_shas: &BTreeSet<String>,
) -> BTreeSet<String> {
    manifest
        .atom_chunks
        .iter()
        .filter(|chunk| {
            chunk.pack.is_none()
                && !candidate_shas.contains(chunk.object_sha256())
                && cas_missing_shas.contains(chunk.object_sha256())
        })
        .map(|chunk| chunk.sha256.clone())
        .collect()
}

/// Drop CAS-proven unbackable atom digests from a held cut and attach an
/// authenticated retirement receipt. Returns how many digests were retired.
///
/// Used when a sticky cut drained its candidates but CAS still shortfalls on
/// carried-forward ghosts: listing at cut time kept them (or reconcile was
/// incomplete), HEAD after reseed proves them absent, and no amount of drain
/// can upload them. Mutating the held cut in place (same generation) avoids
/// the infinite "must re-cut" livelock that left generation 502 stuck on the
/// primary (2026-08-06).
pub fn apply_cas_proven_unbackable_retirement(
    manifest: &mut BackupManifest,
    previous: Option<&BackupManifest>,
    ghost_shas: &BTreeSet<String>,
    authorized_at_unix_secs: u64,
) -> usize {
    if ghost_shas.is_empty() {
        return 0;
    }
    let retired = retire_atom_versions(&mut manifest.atom_chunks, previous, ghost_shas);
    if retired.is_empty() {
        return 0;
    }
    // Distinct digests on the receipt (refs may share a sha).
    let retired_count = retired.len();
    manifest
        .deletion_receipts
        .push(BackupDeletionReceipt::new_unbackable_atom_retirement(
            previous.map_or(0, |m| m.counter),
            manifest.counter,
            retired,
            authorized_at_unix_secs.max(1),
        ));
    retired_count
}

/// Absence evidence belongs to a digest, not to its logical chunk identity.
/// A cut can replace an inherited chunk with a resealed version under the
/// same key. If that version vanishes, retain the exact predecessor ref; the
/// caller must probe it independently before it can be retired or published.
pub(super) fn retire_atom_versions(
    chunks: &mut Vec<BackupChunkRef>,
    previous: Option<&BackupManifest>,
    missing_shas: &BTreeSet<String>,
) -> Vec<String> {
    let previous_by_key: BTreeMap<_, _> = previous
        .into_iter()
        .flat_map(|m| &m.atom_chunks)
        .map(|chunk| (chunk_key(chunk), chunk))
        .collect();
    let mut retired = BTreeSet::new();
    chunks.retain_mut(|chunk| {
        if !missing_shas.contains(&chunk.sha256) {
            return true;
        }
        retired.insert(chunk.sha256.clone());
        if let Some(old) = previous_by_key.get(&chunk_key(chunk)) {
            if old.sha256 != chunk.sha256 {
                *chunk = (*old).clone();
                return true;
            }
        }
        false
    });
    retired.into_iter().collect()
}

/// Digests named by the held cut that exist nowhere: HEAD-missing after
/// reseed, and either not a local candidate or a candidate whose sealed file
/// is already gone. Names still in the last finished cloud photograph are
/// not holes — they do not appear in `cas_missing_shas`.
#[must_use]
pub fn cas_proven_named_hole_shas(
    manifest: &BackupManifest,
    candidate_shas: &BTreeSet<String>,
    cas_missing_shas: &BTreeSet<String>,
    source_missing_shas: &BTreeSet<String>,
) -> BTreeSet<String> {
    let leftover = manifest
        .atom_chunks
        .iter()
        .chain(manifest.mutable_chunks.iter())
        .filter(|chunk| {
            chunk.pack.is_none()
                && cas_missing_shas.contains(chunk.object_sha256())
                && (!candidate_shas.contains(chunk.object_sha256())
                    || source_missing_shas.contains(chunk.object_sha256()))
        })
        .map(|chunk| chunk.sha256.clone());
    leftover.collect()
}

/// Record leftover names as named holes on the held cut, drop them from the
/// chunk lists, and attach an exclusion receipt so the stamp can publish.
/// Returns how many distinct digests became holes.
pub fn apply_named_hole_exclusions(
    manifest: &mut BackupManifest,
    previous: Option<&BackupManifest>,
    hole_shas: &BTreeSet<String>,
    authorized_at_unix_secs: u64,
) -> usize {
    if hole_shas.is_empty() {
        return 0;
    }
    let mut holes: Vec<BackupNamedHole> = manifest
        .atom_chunks
        .iter()
        .chain(manifest.mutable_chunks.iter())
        .filter(|chunk| hole_shas.contains(&chunk.sha256))
        .map(|chunk| BackupNamedHole {
            sha256: chunk.sha256.clone(),
            collection: chunk.collection.clone(),
            role: chunk.role,
        })
        .collect();
    // Source-missing candidates may already have been dropped from the lists
    // if they were only candidates; still record the digest.
    for sha in hole_shas {
        if !holes.iter().any(|h| h.sha256 == *sha) {
            holes.push(BackupNamedHole {
                sha256: sha.clone(),
                collection: String::new(),
                role: BackupManifestRole::Mutable,
            });
        }
    }
    holes.sort_by(|a, b| a.sha256.cmp(&b.sha256));
    holes.dedup_by(|a, b| a.sha256 == b.sha256);
    if holes.is_empty() {
        return 0;
    }
    let retired: Vec<String> = holes.iter().map(|h| h.sha256.clone()).collect();
    let retired_count = retired.len();
    retire_atom_versions(&mut manifest.atom_chunks, previous, hole_shas);
    manifest
        .mutable_chunks
        .retain(|chunk| !hole_shas.contains(&chunk.sha256));
    manifest.named_holes.extend(holes);
    manifest.named_holes.sort_by(|a, b| a.sha256.cmp(&b.sha256));
    manifest.named_holes.dedup_by(|a, b| a.sha256 == b.sha256);
    manifest
        .deletion_receipts
        .push(BackupDeletionReceipt::new_named_hole_exclusion(
            previous.map_or(0, |m| m.counter),
            manifest.counter,
            retired,
            authorized_at_unix_secs.max(1),
        ));
    retired_count
}
