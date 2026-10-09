use std::cmp::Ordering;

use schema_types::Schema;

use super::near_miss::NearMissDecision;
use super::types::DescriptiveNameConflict;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MatchSeam {
    IdentityHash,
    NameExact,
    NameSemantic,
    FieldOverlap,
    PurposeReuse,
}

impl MatchSeam {
    fn priority(self) -> u8 {
        match self {
            Self::IdentityHash => 0,
            Self::NameExact => 1,
            Self::NameSemantic => 2,
            Self::FieldOverlap => 3,
            Self::PurposeReuse => 4,
        }
    }

    pub(super) fn single_signal_decision(self) -> NearMissDecision {
        match self {
            Self::IdentityHash => NearMissDecision::AlreadyExists,
            Self::NameExact | Self::NameSemantic | Self::FieldOverlap | Self::PurposeReuse => {
                NearMissDecision::Expanded
            }
        }
    }

    pub(super) fn veto_decision(self) -> NearMissDecision {
        match self {
            Self::IdentityHash => NearMissDecision::DescriptiveNameConflict,
            Self::NameExact | Self::NameSemantic | Self::FieldOverlap | Self::PurposeReuse => {
                NearMissDecision::Added
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct MatchCandidate {
    pub(super) seam: MatchSeam,
    pub(super) existing_hash: String,
    pub(super) existing: Schema,
    pub(super) target_descriptive_name: String,
    pub(super) score: f32,
}

impl MatchCandidate {
    pub(super) fn new(
        seam: MatchSeam,
        existing_hash: String,
        existing: Schema,
        target_descriptive_name: String,
        score: f32,
    ) -> Self {
        Self {
            seam,
            existing_hash,
            existing,
            target_descriptive_name,
            score,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum CanonicalizationGateOutcome {
    Merge,
    RescueRetry,
    DeCollideAndRegister,
}

#[derive(Clone, Debug)]
pub(super) struct CandidateConflict {
    pub(super) conflict: DescriptiveNameConflict,
}

#[derive(Clone, Debug, Default)]
pub(super) struct CandidateSet {
    pub(super) candidates: Vec<MatchCandidate>,
    pub(super) conflict: Option<CandidateConflict>,
}

pub(super) fn rank_candidates(candidates: &[MatchCandidate]) -> Option<MatchCandidate> {
    candidates.iter().cloned().min_by(compare_candidates)
}

fn compare_candidates(left: &MatchCandidate, right: &MatchCandidate) -> Ordering {
    left.seam
        .priority()
        .cmp(&right.seam.priority())
        .then_with(|| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| left.existing_hash.cmp(&right.existing_hash))
}

pub(super) fn gate_outcome_for_candidate(
    candidate: &MatchCandidate,
    strict_gate_allows_merge: bool,
) -> CanonicalizationGateOutcome {
    if strict_gate_allows_merge || candidate.seam == MatchSeam::PurposeReuse {
        return CanonicalizationGateOutcome::Merge;
    }

    match candidate.seam {
        MatchSeam::NameExact | MatchSeam::NameSemantic | MatchSeam::FieldOverlap => {
            CanonicalizationGateOutcome::RescueRetry
        }
        MatchSeam::IdentityHash => CanonicalizationGateOutcome::DeCollideAndRegister,
        MatchSeam::PurposeReuse => CanonicalizationGateOutcome::Merge,
    }
}

/// One app registered both schemas and gave them different descriptive names:
/// they are two schemas, whatever their field descriptions say.
///
/// Found 2026-09-22: lastgit registered `LastgitRepoIndex` (repo-list rollup)
/// and the service merged it into `LastgitOpenCrIndex` (open-CR rollup)
/// because `semantic_field_rename_map` matched the shared `updated_at`
/// description. Two app schemas then shared one identity and co-owned key
/// slots (papercut-schema-service-merges-distinct-same-app-schemas-by-description).
/// The owning app's distinct names are the strongest signal the service
/// has, so they veto a similarity merge. An exact identity-hash match
/// cannot carry two names, and names that differ only in ASCII case are
/// treated as the same name. A multi-key sibling (same product, different
/// lookup keys) is decided before this check and keeps its field mapping.
pub(super) fn same_app_distinct_name_veto(incoming: &Schema, candidate: &MatchCandidate) -> bool {
    if candidate.seam == MatchSeam::IdentityHash {
        return false;
    }
    let owner = |s: &Schema| {
        s.owner_app_id
            .as_deref()
            .map(str::trim)
            .filter(|o| !o.is_empty())
            .map(str::to_owned)
    };
    let (Some(incoming_owner), Some(existing_owner)) =
        (owner(incoming), owner(&candidate.existing))
    else {
        return false;
    };
    if incoming_owner != existing_owner {
        return false;
    }
    let name = |s: &Schema| {
        s.descriptive_name
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_owned()
    };
    let (incoming_name, existing_name) = (name(incoming), name(&candidate.existing));
    !incoming_name.is_empty()
        && !existing_name.is_empty()
        && !incoming_name.eq_ignore_ascii_case(&existing_name)
}
