//! Exemem's per-database cutover commit.
//!
//! The same object is the commit in DEV and in PROD. A local file is not a
//! receipt. This module does not verify the service signature.

use super::proof::ShareEnvironment;
use super::Ed25519Signature;
use crate::v2::Version;
use crate::{input_digest, DecimalU64, Digest, Id, ProtocolError};
use serde::{Deserialize, Serialize};

/// Domain for [`CutoverReceipt::digest`].
pub const SHARE_CUTOVER_RECEIPT_DOMAIN: &str = "lastdb:share:cutover-receipt:v1";

/// Which node-read digest a database accepts.
///
/// The stored receipt decides. The service realm does not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptedProof {
    /// No receipt. The body-less pre-migration proof is the only one accepted.
    Legacy,
    /// A receipt is stored. The raw-body proof is the only one accepted.
    SignedBody,
}

/// Exemem's signed commit that one database has left the legacy proof.
///
/// `evidence_sha256` is the evidence digest the cutover checker already
/// computed. This object does not recompute those evidence lines. The service
/// key signs `digest`. That key is not the publisher key and not the node key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutoverReceipt {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Realm of this commit. A DEV receipt is not a PROD commit.
    pub environment: ShareEnvironment,
    /// Database this commit switches.
    pub database_id: Id,
    /// SHA-256 of the cutover evidence. Not a timestamp.
    pub evidence_sha256: Digest,
    /// Member-list revision named by that evidence. Zero is rejected.
    pub grant_revision: DecimalU64,
    /// Service signature over the digest hex. It is not part of the digest.
    pub signature: Ed25519Signature,
}

impl CutoverReceipt {
    /// Reject a zero grant revision.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.grant_revision.get() == 0 {
            return Err(ProtocolError::invalid("share.cutover.grant_revision"));
        }
        Ok(())
    }

    /// Domain-separated SHA-256 of this receipt with the signature removed.
    ///
    /// The array is `version`, `environment`, `database_id`, `evidence_sha256`,
    /// `grant_revision`.
    pub fn digest(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        input_digest(
            SHARE_CUTOVER_RECEIPT_DOMAIN,
            &(
                &self.version,
                self.environment,
                &self.database_id,
                &self.evidence_sha256,
                self.grant_revision,
            ),
        )
    }
}

/// Select the only digest this database accepts.
///
/// `None` is the pre-migration proof. `Some` is the raw-body proof, including
/// when the receipt was minted in the other realm. The caller still checks
/// that realm against the service it is running.
#[must_use]
pub fn accepted_proof(receipt: Option<&CutoverReceipt>) -> AcceptedProof {
    if receipt.is_some() {
        AcceptedProof::SignedBody
    } else {
        AcceptedProof::Legacy
    }
}
