//! The peer-apply interlock of the automatic cadence.
//!
//! Automatic delete runs only on a node that has NEVER applied a mutation a
//! peer wrote. The replay path that modern peers use records neither side of a
//! multi-device conflict, so the loser is an ordinary unreferenced atom. The
//! state is one durable row (see `db_operations::atom_store::peer_apply`); a
//! home with no row is `unknown`, and unknown counts as peer-applied.

use super::*;
use crate::db_operations::PeerApplyState;

/// What a step learned about peer data before it decides to delete.
pub(super) struct PeerGate {
    /// `Some` when delete must stay off this step: the skip reason to report.
    pub(super) interlock_reason: Option<&'static str>,
}

impl AutomaticGcAtomsRuntime<'_> {
    /// Read the durable state, publish it in the status, and say whether the
    /// interlock holds delete off. A read error is `unknown`: not knowing is not
    /// a pass. The probe and the unreferenced figure run either way.
    pub(super) async fn peer_apply_gate(&self, settings: &AutomaticGcAtomsSettings) -> PeerGate {
        let (label, reason) = match self.atoms.peer_apply_state().await {
            Ok(PeerApplyState::Never) => ("never", None),
            Ok(PeerApplyState::Seen) => ("seen", Some(skip_reason::PEER_APPLIED)),
            Ok(PeerApplyState::Unknown) => ("unknown", Some(skip_reason::PEER_STATE_UNKNOWN)),
            Err(error) => {
                warn!(
                    target: "fold_db::gc_atoms",
                    %error,
                    "automatic gc-atoms could not read the peer-apply state; treating it as unknown"
                );
                ("unknown", Some(skip_reason::PEER_STATE_UNKNOWN))
            }
        };
        let changed = self
            .status
            .record_peer_state(label, settings.allow_after_peer_apply);
        let interlock_reason = reason.filter(|_| !settings.allow_after_peer_apply);
        if changed {
            match (interlock_reason, settings.allow_after_peer_apply) {
                (Some(reason), _) => info!(
                    target: "fold_db::gc_atoms",
                    peer_apply_state = label,
                    reason,
                    "automatic gc-atoms measures but does not delete: the peer-apply interlock is on"
                ),
                (None, true) if label != "never" => warn!(
                    target: "fold_db::gc_atoms",
                    peer_apply_state = label,
                    "LASTDB_GC_ATOMS_ALLOW_AFTER_PEER_APPLY is set: automatic gc-atoms may delete the atoms of lost multi-device writes"
                ),
                _ => {}
            }
        }
        PeerGate { interlock_reason }
    }
}
