//! Parse evidence bytes. A parse failure names the file and does not echo the body.

use serde::de::DeserializeOwned;

use crate::error::{Reason, Refusal};
use crate::record::{Request, REQUEST_FILE};
use crate::wire::{
    Bundle, SecondDeviceProof, DAEMON_READY_FILE, DELIVERY_READY_FILE, FINAL_WRITES_FILE,
    GRANTS_FILE, LEGACY_POSITION_FILE, MAX_EVIDENCE_BYTES, SNAPSHOT_FILE,
};

/// The six evidence files, as bytes the shell already read.
pub struct EvidenceBytes<'a> {
    /// `snapshot.json`.
    pub snapshot: &'a [u8],
    /// `legacy-position.json`.
    pub legacy_position: &'a [u8],
    /// `final-writes.json`.
    pub final_writes: &'a [u8],
    /// `grants.json`.
    pub grants: &'a [u8],
    /// `daemon-ready.json`.
    pub daemon_ready: &'a [u8],
    /// `delivery-ready.json`.
    pub delivery_ready: &'a [u8],
}

/// Parse the six files. Semantic checks are [`crate::verify`], not this function.
pub fn parse_bundle(files: &EvidenceBytes<'_>) -> Result<Bundle, Refusal> {
    let mut reasons = Vec::new();
    let snapshot = parse_one(SNAPSHOT_FILE, files.snapshot, &mut reasons);
    let legacy = parse_one(LEGACY_POSITION_FILE, files.legacy_position, &mut reasons);
    let writes = parse_one(FINAL_WRITES_FILE, files.final_writes, &mut reasons);
    let grants = parse_one(GRANTS_FILE, files.grants, &mut reasons);
    let daemon = parse_one(DAEMON_READY_FILE, files.daemon_ready, &mut reasons);
    let delivery = parse_one(DELIVERY_READY_FILE, files.delivery_ready, &mut reasons);
    if let Some(refusal) = Refusal::new(reasons) {
        return Err(refusal);
    }
    if let (
        Some(snapshot),
        Some(legacy),
        Some(writes),
        Some(grants),
        Some(daemon),
        Some(delivery),
    ) = (snapshot, legacy, writes, grants, daemon, delivery)
    {
        Ok(Bundle {
            snapshot,
            legacy,
            writes,
            grants,
            daemon,
            delivery,
        })
    } else {
        Err(Refusal::one(Reason::Unreadable {
            file: SNAPSHOT_FILE,
        }))
    }
}

/// Parse a second-device proof. The body is not included in the error.
pub fn parse_proof(bytes: &[u8]) -> Result<SecondDeviceProof, Refusal> {
    let mut reasons = Vec::new();
    let proof = parse_one("second-device.json", bytes, &mut reasons);
    if let Some(refusal) = Refusal::new(reasons) {
        return Err(refusal);
    }
    proof.ok_or_else(|| {
        Refusal::one(Reason::Unreadable {
            file: "second-device.json",
        })
    })
}

/// Parse an inactive request. A signed-only file, a receipt object, or a retirement grant fails.
pub fn parse_request(bytes: &[u8]) -> Result<Request, Refusal> {
    let mut reasons = Vec::new();
    let request = parse_one::<Request>(REQUEST_FILE, bytes, &mut reasons);
    if let Some(refusal) = Refusal::new(reasons) {
        return Err(refusal);
    }
    let Some(request) = request else {
        return Err(Refusal::one(Reason::Unreadable { file: REQUEST_FILE }));
    };
    request.consistent().map_err(Refusal::one)?;
    Ok(request)
}

fn parse_one<T: DeserializeOwned>(
    file: &'static str,
    bytes: &[u8],
    reasons: &mut Vec<Reason>,
) -> Option<T> {
    if bytes.len() > MAX_EVIDENCE_BYTES {
        reasons.push(Reason::Unreadable { file });
        return None;
    }
    match serde_json::from_slice(bytes) {
        Ok(value) => Some(value),
        Err(_) => {
            reasons.push(Reason::Unreadable { file });
            None
        }
    }
}
