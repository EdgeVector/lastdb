use super::SnapshotAdoptionRequirement as Requirement;
use super::{SnapshotAdoptionOrigin, SnapshotAdoptionPrefix, SnapshotAdoptionReplaySource};

pub(super) fn for_origins(origins: &[SnapshotAdoptionOrigin]) -> Vec<Requirement> {
    // Six unconditional requirements and three conditional requirements: never
    // a caller-controlled list or permission to publish a replacement.
    let mut requirements = vec![
        Requirement::SharedBaseProof,
        Requirement::CandidateNativeVerification,
        Requirement::LocalCutRevalidation,
        Requirement::LocalIdentityAndAnchorPreservation,
        Requirement::ReceiptBaseAndControlRewrite,
        Requirement::AtomicPublication,
    ];
    if origins.iter().any(|origin| {
        origin.replay.as_ref().is_some_and(|replay| {
            replay.source == SnapshotAdoptionReplaySource::LocalAuthoredStream
        })
    }) {
        requirements.push(Requirement::OwnTailReplay);
    }
    if origins.iter().any(|origin| {
        origin.replay.as_ref().is_some_and(|replay| {
            replay.source == SnapshotAdoptionReplaySource::OriginalSourceDevice
        })
    }) {
        requirements.push(Requirement::OriginalSourceTails);
    }
    if origins.iter().any(|origin| {
        matches!(
            &origin.prefix,
            SnapshotAdoptionPrefix::CandidatePrefixProofRequired { .. }
        )
    }) {
        requirements.push(Requirement::CandidatePrefixProof);
    }
    requirements
}
