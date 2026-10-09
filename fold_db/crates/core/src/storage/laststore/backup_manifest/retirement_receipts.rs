//! Validation of authenticated retirement receipts that sanction a chunk set
//! shrinking between two manifests.

use super::*;

/// Unbackable retirement is valid only when every removed atom's sha256 is
/// listed on an authenticated receipt covering previous→current counters.
pub(super) fn has_valid_unbackable_atom_retirement_receipt(
    previous: &BackupManifest,
    current: &BackupManifest,
    removed_keys: &BTreeSet<(String, u16, Option<u32>, String)>,
) -> bool {
    if removed_keys.is_empty() {
        return false;
    }
    let previous_by_key: BTreeMap<_, _> = previous
        .atom_chunks
        .iter()
        .map(|chunk| (chunk_key(chunk), chunk))
        .collect();
    let mut removed_shas = BTreeSet::new();
    for key in removed_keys {
        let Some(chunk) = previous_by_key.get(key) else {
            return false;
        };
        removed_shas.insert(chunk.sha256.as_str());
    }

    current.deletion_receipts.iter().any(|receipt| {
        if receipt.record_type != UNBACKABLE_ATOM_RETIREMENT_RECEIPT_TYPE {
            return false;
        }
        if receipt.reason.as_deref() != Some(UNBACKABLE_RETIREMENT_REASON_ABSENT_LOCAL_AND_CLOUD) {
            return false;
        }
        if receipt.authorized_at_unix_secs == 0 {
            return false;
        }
        if receipt.floor_from_manifest_counter != previous.counter
            || receipt.floor_to_manifest_counter != current.counter
            || receipt.floor_to_manifest_counter <= receipt.floor_from_manifest_counter
        {
            return false;
        }
        let receipt_shas: BTreeSet<_> = receipt
            .retired_atom_chunk_shas
            .iter()
            .map(String::as_str)
            .collect();
        removed_shas.iter().all(|sha| receipt_shas.contains(sha))
    })
}

pub(super) fn has_valid_purged_atom_retirement_receipt(
    previous: &BackupManifest,
    current: &BackupManifest,
    removed_keys: &BTreeSet<(String, u16, Option<u32>, String)>,
) -> bool {
    if removed_keys.is_empty() {
        return false;
    }
    let previous_by_key: BTreeMap<_, _> = previous
        .atom_chunks
        .iter()
        .map(|chunk| (chunk_key(chunk), chunk))
        .collect();
    let mut removed_shas = BTreeSet::new();
    for key in removed_keys {
        let Some(chunk) = previous_by_key.get(key) else {
            return false;
        };
        removed_shas.insert(chunk.sha256.as_str());
    }

    current.deletion_receipts.iter().any(|receipt| {
        receipt.record_type == PURGED_ATOM_RETIREMENT_RECEIPT_TYPE
            && receipt.reason.as_deref() == Some(PURGED_ATOM_RETIREMENT_REASON)
            && receipt.user_authorized
            && receipt.authorized_at_unix_secs > 0
            && receipt.floor_from_manifest_counter == previous.counter
            && receipt.floor_to_manifest_counter == current.counter
            && receipt.floor_to_manifest_counter > receipt.floor_from_manifest_counter
            && {
                let receipt_shas: BTreeSet<_> = receipt
                    .retired_atom_chunk_shas
                    .iter()
                    .map(String::as_str)
                    .collect();
                removed_shas.iter().all(|sha| receipt_shas.contains(sha))
            }
    })
}

pub(super) fn named_hole_or_retirement_receipt_covers(
    receipt: &BackupDeletionReceipt,
    previous_counter: u64,
    current_counter: u64,
) -> bool {
    let type_ok = receipt.record_type == NAMED_HOLE_EXCLUSION_RECEIPT_TYPE
        || receipt.record_type == UNBACKABLE_ATOM_RETIREMENT_RECEIPT_TYPE
        || receipt.record_type == PURGED_ATOM_RETIREMENT_RECEIPT_TYPE;
    type_ok
        && receipt.reason.is_some()
        && receipt.authorized_at_unix_secs > 0
        && receipt.floor_from_manifest_counter == previous_counter
        && receipt.floor_to_manifest_counter == current_counter
        && receipt.floor_to_manifest_counter > receipt.floor_from_manifest_counter
}

pub(super) fn removed_atom_shas(
    previous: &BackupManifest,
    removed_keys: &BTreeSet<(String, u16, Option<u32>, String)>,
) -> Option<BTreeSet<String>> {
    let previous_by_key: BTreeMap<_, _> = previous
        .atom_chunks
        .iter()
        .map(|chunk| (chunk_key(chunk), chunk))
        .collect();
    let mut removed_shas = BTreeSet::new();
    for key in removed_keys {
        let chunk = previous_by_key.get(key)?;
        removed_shas.insert(chunk.sha256.clone());
    }
    Some(removed_shas)
}

/// Named-hole receipt covering every removed atom digest.
pub(super) fn has_valid_named_hole_exclusion_receipt(
    previous: &BackupManifest,
    current: &BackupManifest,
    removed_keys: &BTreeSet<(String, u16, Option<u32>, String)>,
) -> bool {
    if removed_keys.is_empty() {
        return false;
    }
    let Some(removed_shas) = removed_atom_shas(previous, removed_keys) else {
        return false;
    };
    current.deletion_receipts.iter().any(|receipt| {
        if receipt.record_type != NAMED_HOLE_EXCLUSION_RECEIPT_TYPE {
            return false;
        }
        if receipt.reason.as_deref() != Some(NAMED_HOLE_REASON_ABSENT_LOCAL_AND_CLOUD) {
            return false;
        }
        if receipt.authorized_at_unix_secs == 0 {
            return false;
        }
        if receipt.floor_from_manifest_counter != previous.counter
            || receipt.floor_to_manifest_counter != current.counter
            || receipt.floor_to_manifest_counter <= receipt.floor_from_manifest_counter
        {
            return false;
        }
        let receipt_shas: BTreeSet<_> = receipt
            .retired_atom_chunk_shas
            .iter()
            .map(String::as_str)
            .collect();
        removed_shas
            .iter()
            .all(|sha| receipt_shas.contains(sha.as_str()))
    })
}

/// Union of unbackable / purged / named-hole receipts covering every removed
/// atom digest. Needed when a held cut retires atoms in one pass and holes
/// leftovers in another — neither receipt lists the full removed set.
pub(super) fn retirement_receipts_cover_removed_atoms(
    previous: &BackupManifest,
    current: &BackupManifest,
    removed_keys: &BTreeSet<(String, u16, Option<u32>, String)>,
) -> bool {
    if removed_keys.is_empty() {
        return false;
    }
    let Some(removed_shas) = removed_atom_shas(previous, removed_keys) else {
        return false;
    };
    let mut covered = BTreeSet::new();
    for receipt in &current.deletion_receipts {
        if named_hole_or_retirement_receipt_covers(receipt, previous.counter, current.counter) {
            covered.extend(receipt.retired_atom_chunk_shas.iter().cloned());
        }
    }
    removed_shas.iter().all(|sha| covered.contains(sha))
}
