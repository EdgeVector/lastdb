//! Validate evidence and prepare one inactive request.
//!
//! Nothing here contacts Exemem or changes access. A second-device proof is
//! checked and not stored. Retirement stays forbidden.

use crate::check::{canonical_position, label_ok, problems, snapshot_ref_ok};
use crate::digest::digest;
use crate::error::{Reason, Refusal};
use crate::record::{new_request, Request, REQUEST_FILE};
use crate::wire::{Bundle, SecondDeviceProof, SCHEMA};

/// A new inactive request, or the same request when the evidence digest matches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrepareOutcome {
    /// The shell may store this request. It is not a signed-only commit.
    Ready(Request),
    /// The stored request already binds this evidence. Do not write again.
    Unchanged(Request),
}

/// Check one bundle. The returned string is the evidence digest.
pub fn verify(expected_database_id: &str, bundle: &Bundle) -> Result<String, Refusal> {
    if let Some(refusal) = Refusal::new(problems(expected_database_id, bundle)) {
        return Err(refusal);
    }
    Ok(digest(bundle))
}

/// Prepare one inactive request, or return the stored request when the digest matches.
///
/// Different evidence does not overwrite. This does not set `signed_only`.
pub fn prepare(
    expected_database_id: &str,
    existing: Option<&Request>,
    bundle: &Bundle,
) -> Result<PrepareOutcome, Refusal> {
    let digest = verify(expected_database_id, bundle)?;
    let Some(existing) = existing else {
        return Ok(PrepareOutcome::Ready(new_request(bundle, &digest)));
    };
    if existing.database_id != expected_database_id {
        return Err(refusal(Reason::Database { file: REQUEST_FILE }));
    }
    if existing.evidence_sha256 != digest {
        return Err(refusal(Reason::RequestDiffers));
    }
    if !same_binding(existing, bundle) {
        return Err(refusal(Reason::RecordInconsistent));
    }
    existing.consistent().map_err(refusal)?;
    Ok(PrepareOutcome::Unchanged(existing.clone()))
}

/// A readable request does not change legacy resume for this database.
///
/// A missing file is not this function. The shell treats absence as unchanged
/// access. Exemem has not committed.
pub fn resume_unchanged(request: &Request, database_id: &str) -> Result<(), Refusal> {
    if request.database_id != database_id {
        return Err(refusal(Reason::Database { file: REQUEST_FILE }));
    }
    request.consistent().map_err(refusal)?;
    Ok(())
}

/// Check a second-device ack and matching keyed reads. This writes nothing.
///
/// Success does not allow retirement and does not activate signed-only.
pub fn check_proof(request: &Request, proof: &SecondDeviceProof) -> Result<(), Refusal> {
    request.consistent().map_err(refusal)?;
    let mut reasons = Vec::new();
    if !proof_matches(request, proof) {
        reasons.push(Reason::SecondDevice);
    }
    if proof.device_id == request.migrating_device_id {
        reasons.push(Reason::SameDevice);
    }
    if let Some(refusal) = Refusal::new(reasons) {
        return Err(refusal);
    }
    Ok(())
}

fn same_binding(request: &Request, bundle: &Bundle) -> bool {
    request.database_kind == bundle.snapshot.database_kind
        && request.snapshot_ref == bundle.snapshot.snapshot_ref
        && request.final_legacy_position == bundle.legacy.position
        && request.authority_node_key == bundle.grants.authority_node_key
        && request.grant_revision == bundle.grants.revision
        && request.migrating_device_id == bundle.daemon.migrating_device_id
}

fn proof_matches(request: &Request, proof: &SecondDeviceProof) -> bool {
    proof.kind == "second_device_proof"
        && proof.schema == SCHEMA
        && proof.environment == "dev"
        && proof.database_id == request.database_id
        && label_ok(&proof.device_id)
        && proof.keyed_reads_match
        && snapshot_ref_ok(&request.snapshot_ref)
        && canonical_position(&proof.acked_through_legacy_position).is_some()
        && proof.acked_through_legacy_position == request.final_legacy_position
        && empty_matches(request, proof)
}

fn empty_matches(request: &Request, proof: &SecondDeviceProof) -> bool {
    let Some(position) = canonical_position(&request.final_legacy_position) else {
        return false;
    };
    if position == 0 {
        proof.empty_database && proof.compared_keys == 0
    } else {
        !proof.empty_database && proof.compared_keys > 0
    }
}

fn refusal(reason: Reason) -> Refusal {
    Refusal::one(reason)
}
