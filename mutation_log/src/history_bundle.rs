//! Bundle apply path for [`super::VerifiedHistory`].

use crate::accept::{AcceptOutcome, PendingReason, RejectReason};
use crate::bundle::{validate_bundle_changes, BundleChange};
use crate::entry::SignedEntry;
use crate::wire::entry_hash;

use super::VerifiedHistory;
use crate::accept::position_of;

impl VerifiedHistory {
    /// Apply decrypted bundle inner changes only (caller verified blob I/O).
    pub fn apply_bundle_changes(
        &mut self,
        entry: &SignedEntry,
        changes: &[BundleChange],
    ) -> Result<(), RejectReason> {
        let SignedEntry::Bundle { body, .. } = entry else {
            return Err(RejectReason::Malformed("not a bundle entry"));
        };
        validate_bundle_changes(body, changes)?;
        let position = position_of(entry);
        for ch in changes {
            let payload = crate::payload::encode_record_payload(&ch.record_key, &ch.value);
            self.merge_data(&ch.namespace, &payload, position);
        }
        Ok(())
    }

    /// Run AcceptEntry checks then apply bundle metadata and inner changes.
    pub fn accept_bundle_with_changes(
        &mut self,
        entry: SignedEntry,
        changes: &[BundleChange],
    ) -> AcceptOutcome {
        let outcome = self.evaluate_entry(&entry);
        match &outcome {
            AcceptOutcome::Accept {
                duplicate: true, ..
            } => return outcome,
            AcceptOutcome::Reject { .. } => return outcome,
            AcceptOutcome::Pending {
                reason: PendingReason::BundleBlob { .. },
            } => {}
            AcceptOutcome::Pending { .. } | AcceptOutcome::Accept { .. } => return outcome,
        }
        let SignedEntry::Bundle { body, .. } = &entry else {
            return AcceptOutcome::Reject {
                reason: RejectReason::Malformed("accept_bundle_with_changes requires bundle"),
            };
        };
        if let Err(reason) = validate_bundle_changes(body, changes) {
            return AcceptOutcome::Reject { reason };
        }
        let hash = match entry_hash(&entry) {
            Ok(h) => h,
            Err(e) => {
                return AcceptOutcome::Reject {
                    reason: RejectReason::Encode(e),
                };
            }
        };
        if let Err(reason) = self.apply_bundle_changes(&entry, changes) {
            return AcceptOutcome::Reject { reason };
        }
        self.drop_pending_bundle(&hash);
        self.apply_accepted(hash, entry);
        self.drain_pending();
        AcceptOutcome::Accept {
            entry_hash: hash,
            duplicate: false,
        }
    }
}
