use super::*;

mod exact;
mod gates;
mod matching;
mod proposals;
mod similar;

pub use proposals::SchemaResolveReply;
pub use similar::{accepted_similarity_threshold, SimilarSchemasReply};

/// Bound `POST /v1/schemas/resolve` so the public read path cannot turn one
/// request into unbounded semantic matching work.
pub const MAX_SCHEMA_RESOLVE_PROPOSALS: usize = 50;

/// Resolve confidence: the match class, scaled by field coverage when the
/// match leaves proposal fields unmapped.
///
/// The class alone reported a fixed 0.6 for every partial, non-exact match,
/// so a hit that mapped 4 of 7 fields read as "60% confident" (2026-09-22,
/// LastgitRef onto LastgitPack). Consumers only report this number.
pub(super) fn resolve_match_confidence(
    reuse_match: &SchemaReuseMatch,
    proposal_field_count: usize,
) -> f32 {
    let class = match (reuse_match.is_exact_match, reuse_match.is_superset) {
        (true, true) => 1.0,
        (false, true) => 0.9,
        (true, false) => 0.75,
        (false, false) => 0.6,
    };
    if reuse_match.is_superset || proposal_field_count == 0 {
        return class;
    }
    let covered = proposal_field_count.saturating_sub(reuse_match.unmapped_fields.len());
    class * covered as f32 / proposal_field_count as f32
}
