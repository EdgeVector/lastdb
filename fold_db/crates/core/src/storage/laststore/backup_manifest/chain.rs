//! Manifest and descriptor generation-chain validation.

use super::*;

type PackSlices<'a> = BTreeMap<&'a str, (u64, Vec<(u64, u64, &'a str)>)>;

/// SHA-256 over the manifest's canonical JSON representation.
pub fn manifest_sha256_hex(manifest: &BackupManifest) -> StorageResult<String> {
    let bytes = serde_json::to_vec(manifest)
        .map_err(|e| StorageError::BackendError(format!("manifest encode failed: {e}")))?;
    Ok(sha256_hex(&bytes))
}

/// Validate the generation step from `previous` to `current`.
pub fn validate_manifest_chain(
    previous: Option<&BackupManifest>,
    current: &BackupManifest,
) -> StorageResult<()> {
    classify_manifest_chain_step(previous, current).map(|_| ())
}

/// Validate a cut before the publisher replaces packs with removed members.
/// All chain and per-file checks remain active; only pack coverage waits.
pub fn validate_manifest_chain_before_packing(
    previous: Option<&BackupManifest>,
    current: &BackupManifest,
) -> StorageResult<()> {
    classify_manifest_chain_step_inner(previous, current, false).map(|_| ())
}

/// Validate and classify the generation step from `previous` to `current`.
pub fn classify_manifest_chain_step(
    previous: Option<&BackupManifest>,
    current: &BackupManifest,
) -> StorageResult<BackupManifestChainStep> {
    classify_manifest_chain_step_inner(previous, current, true)
}

fn classify_manifest_chain_step_inner(
    previous: Option<&BackupManifest>,
    current: &BackupManifest,
    require_pack_coverage: bool,
) -> StorageResult<BackupManifestChainStep> {
    if !matches!(current.version, MANIFEST_VERSION | PACKED_MANIFEST_VERSION) {
        return Err(StorageError::BackendError(format!(
            "unsupported backup manifest version {}",
            current.version
        )));
    }
    validate_pack_locations(current, require_pack_coverage)?;
    if let Some(previous) = previous {
        if current.version < previous.version {
            return Err(StorageError::BackendError(
                "backup manifest format version regressed".into(),
            ));
        }
        let expected = manifest_sha256_hex(previous)?;
        if current.previous_manifest_sha256.as_deref() != Some(expected.as_str()) {
            return Err(StorageError::BackendError(
                "backup manifest chain hash mismatch".to_string(),
            ));
        }
        if current.store_uuid != previous.store_uuid || current.epoch != previous.epoch {
            return Err(StorageError::BackendError(
                "backup manifest fork fence changed".to_string(),
            ));
        }
        if current.counter <= previous.counter {
            return Err(StorageError::BackendError(
                "backup manifest counter did not increase".to_string(),
            ));
        }
        if current.cut_csn < previous.cut_csn {
            return Err(StorageError::BackendError(
                "backup manifest cut CSN regressed".to_string(),
            ));
        }
        let previous_atoms: BTreeSet<_> = previous.atom_chunks.iter().map(chunk_key).collect();
        let current_atoms: BTreeSet<_> = current.atom_chunks.iter().map(chunk_key).collect();
        if !previous_atoms.is_subset(&current_atoms) {
            let removed_keys: BTreeSet<_> =
                previous_atoms.difference(&current_atoms).cloned().collect();
            if has_valid_unbackable_atom_retirement_receipt(previous, current, &removed_keys) {
                return Ok(BackupManifestChainStep::SanctionedUnbackableAtomRetirement);
            }
            if has_valid_purged_atom_retirement_receipt(previous, current, &removed_keys) {
                return Ok(BackupManifestChainStep::SanctionedPurgedAtomRetirement);
            }
            if has_valid_named_hole_exclusion_receipt(previous, current, &removed_keys) {
                return Ok(BackupManifestChainStep::SanctionedNamedHoleExclusion);
            }
            if retirement_receipts_cover_removed_atoms(previous, current, &removed_keys) {
                return Ok(BackupManifestChainStep::SanctionedNamedHoleExclusion);
            }
            return Err(StorageError::BackendError(
                "backup manifest atom chunk list moved retention floor without authenticated deletion receipt; rollback/truncation attack suspected".to_string(),
            ));
        }
    } else if current.previous_manifest_sha256.is_some() {
        return Err(StorageError::BackendError(
            "first backup manifest must not point at a predecessor".to_string(),
        ));
    }
    Ok(BackupManifestChainStep::OrdinaryAppend)
}

fn validate_pack_locations(
    manifest: &BackupManifest,
    require_pack_coverage: bool,
) -> StorageResult<()> {
    let mut packs: PackSlices<'_> = BTreeMap::new();
    for chunk in manifest.atom_chunks.iter().chain(&manifest.mutable_chunks) {
        let Some(pack) = &chunk.pack else { continue };
        let valid_sha = pack.sha256.len() == 64
            && pack
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
        let valid_range = pack.length > 0
            && pack.length == chunk.bytes
            && pack
                .offset
                .checked_add(pack.length)
                .is_some_and(|end| end <= pack.bytes);
        if manifest.version != PACKED_MANIFEST_VERSION
            || chunk.role != BackupManifestRole::Mutable
            || !valid_sha
            || !valid_range
        {
            return Err(StorageError::BackendError(
                "backup manifest has an invalid pack location".into(),
            ));
        }
        let (bytes, ranges) = packs
            .entry(pack.sha256.as_str())
            .or_insert_with(|| (pack.bytes, Vec::new()));
        if *bytes != pack.bytes {
            return Err(StorageError::BackendError(
                "backup manifest names one pack with different sizes".into(),
            ));
        }
        ranges.push((
            pack.offset,
            pack.offset + pack.length,
            chunk.sha256.as_str(),
        ));
    }
    if !require_pack_coverage {
        return Ok(());
    }
    for (sha, (bytes, mut ranges)) in packs {
        ranges.sort_unstable();
        let mut covered = 0;
        let mut prior = None;
        for range in ranges {
            if prior == Some(range) {
                // Identical files may share one exact slice in a later writer.
                continue;
            }
            if range.0 != covered {
                return Err(StorageError::BackendError(format!(
                    "backup manifest pack {sha} has a gap or overlapping file ranges"
                )));
            }
            covered = range.1;
            prior = Some(range);
        }
        if covered != bytes {
            return Err(StorageError::BackendError(format!(
                "backup manifest pack {sha} has unreferenced bytes"
            )));
        }
    }
    Ok(())
}

/// Classification of one v2 descriptor chain step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescriptorChainStep {
    /// The successor references every predecessor instance.
    OrdinaryAppend,
    /// The successor drops predecessor instances and a receipt names each.
    SanctionedRetirement,
}

/// Validate and classify the step from `previous` to `current` on the v2
/// descriptor chain. This is the v2 twin of [`classify_manifest_chain_step`];
/// the v1 classifier is unchanged and still gates `MANIFEST_VERSION` 1 only.
///
/// Structural rules (landing map 4.2, last row): same scope and store, the
/// authority epoch does not regress, a new root id, and every predecessor
/// instance is either held by the successor or named on `receipt` as retired.
/// A retired instance may not reappear on the successor, and every
/// replacement instance the receipt names must be on the successor.
///
/// Signature checks are the caller's: verify each page with
/// `verify_descriptor_page` and the receipt with
/// `verify_retirement_receipt_v2` before this call. The predecessor link
/// itself (`ROOT.predecessor_root`) lives on the control-plane record, not on
/// the page header, so `previous` is supplied by the caller.
pub fn classify_descriptor_chain_step(
    previous: Option<&DescriptorRootView>,
    current: &DescriptorRootView,
    receipt: Option<&RetirementReceiptV2>,
) -> StorageResult<DescriptorChainStep> {
    let Some(previous) = previous else {
        return Ok(DescriptorChainStep::OrdinaryAppend);
    };
    if current.scope != previous.scope || current.store_uuid != previous.store_uuid {
        return Err(StorageError::BackendError(
            "backup descriptor fork fence changed".to_string(),
        ));
    }
    if current.authority_epoch < previous.authority_epoch {
        return Err(StorageError::BackendError(format!(
            "backup descriptor authority epoch regressed from {} to {}",
            previous.authority_epoch, current.authority_epoch
        )));
    }
    if current.root_id == previous.root_id {
        return Err(StorageError::BackendError(format!(
            "backup descriptor root id {} reused for a successor",
            current.root_id
        )));
    }
    let previous_ids = previous.instance_ids();
    let current_ids = current.instance_ids();
    let removed: BTreeSet<&str> = previous_ids.difference(&current_ids).copied().collect();
    if removed.is_empty() {
        return Ok(DescriptorChainStep::OrdinaryAppend);
    }
    let Some(receipt) = receipt else {
        return Err(StorageError::BackendError(format!(
            "backup descriptor dropped {} predecessor instance(s) without a retirement receipt; rollback/truncation attack suspected",
            removed.len()
        )));
    };
    if receipt.version != DESCRIPTOR_VERSION {
        return Err(StorageError::BackendError(format!(
            "unsupported backup retirement receipt version {}; expected {DESCRIPTOR_VERSION}",
            receipt.version
        )));
    }
    if receipt.scope != current.scope
        || receipt.store_uuid != current.store_uuid
        || receipt.authority_epoch != current.authority_epoch
    {
        return Err(StorageError::BackendError(
            "backup retirement receipt context does not match the successor root".to_string(),
        ));
    }
    if receipt.predecessor_root != previous.root_id || receipt.replacement_root != current.root_id {
        return Err(StorageError::BackendError(format!(
            "backup retirement receipt binds {} -> {}, chain step is {} -> {}",
            receipt.predecessor_root, receipt.replacement_root, previous.root_id, current.root_id
        )));
    }
    if receipt.lineage.first().map(String::as_str) != Some(previous.root_id.as_str()) {
        return Err(StorageError::BackendError(
            "backup retirement receipt lineage does not start at the predecessor root".to_string(),
        ));
    }
    let retired: BTreeSet<&str> = receipt
        .retired_instances
        .iter()
        .map(|instance| instance.instance_id.as_str())
        .collect();
    if let Some(unnamed) = removed.iter().find(|id| !retired.contains(*id)) {
        return Err(StorageError::BackendError(format!(
            "backup descriptor dropped instance {unnamed} that no retirement receipt names; rollback/truncation attack suspected"
        )));
    }
    if let Some(reappeared) = retired.iter().find(|id| current_ids.contains(*id)) {
        return Err(StorageError::BackendError(format!(
            "backup retirement receipt retires instance {reappeared} that the successor still references"
        )));
    }
    if let Some(missing) = receipt
        .replacement_instances
        .iter()
        .find(|id| !current_ids.contains(id.as_str()))
    {
        return Err(StorageError::BackendError(format!(
            "backup retirement receipt names replacement instance {missing} that the successor does not reference"
        )));
    }
    Ok(DescriptorChainStep::SanctionedRetirement)
}
