use super::*;

/// What one mid-hold unbackable retirement actually did to the held cut.
///
/// Returned instead of a bare `bool` so the retain path can report the state
/// the retirement *produced*, not the state it started from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct UnbackableRetirement {
    /// Distinct digests dropped from the held cut on this attempt.
    pub(super) retired: usize,
    /// Manifest chunks still naming no local candidate after the drop.
    pub(super) unbackable_after: usize,
}

/// Max retirement passes against one held cut before we stop and retain.
///
/// A single CAS shortfall can false-positive presence for carried-forward
/// ghosts (stale cache / incomplete reseed). The first retirement removes the
/// digests that were already in the missing set; the re-verify then surfaces
/// previously-cached ghosts as newly missing. Measured on the 2026-08-17
/// primary (gen 508): retired=32, then missing/unbackable=223 — abandoning
/// there discarded the receipts and re-cut from the published previous
/// manifest, which reintroduced every ghost. A second pass against the new
/// missing set is what converges; a hard cap keeps a wedged home from
/// spinning forever inside one CAS attempt.
pub(super) const MAX_UNBACKABLE_RETIREMENT_PASSES: u32 = 8;

/// Operator-facing reason a held cut is retained after retirement(s) that did
/// not yet close the shortfall.
///
/// Every number here is a **pair**: what it was before the retirement work and
/// what it is after. That is not verbosity, it is the whole point of the
/// message.
///
/// Until 2026-08-17 this text carried only the pre-retirement values — both the
/// shortfall (bound by the `Err` of the verify that ran *before* the retirement)
/// and the unbackable count (read off the held target *before* the retirement
/// rewrote it). So a retirement that genuinely dropped 32 ghosts and cut the
/// shortfall from 255 to 223 published itself as `missing=255
/// unbackable_manifest_chunks=255`, and `lastdb status` quoted that verbatim on
/// its `Backup:` line. Observed on the primary that day: the daemon logged
/// `retired=32 remaining_unbackable=223`, then, fourteen minutes later,
/// announced the identical 255/255 it had opened with. A reader — human or
/// agent — could only conclude the retirement was a no-op, which is the same
/// wrong conclusion the earlier non-convergence investigation reached.
///
/// A progress report that cannot show progress is worse than no report: it
/// argues *against* the mechanism that is working.
///
/// As of 2026-08-19 this message no longer claims "held cut abandoned; next
/// cycle will re-cut". Abandoning after a partial retirement discards the
/// in-memory receipts (`papercut-lastdb-unbackable-retirement-is-discarded-
/// when-the-held-cut-is-abandoned`) and the next cut re-imports the same
/// ghosts from the last *published* manifest — the abandon→re-cut thrash that
/// left primary durability on `backup_cut_unbackable` while consecutive_failures
/// climbed. Retaining the narrowed held cut lets the next cycle retry CAS
/// (and further retirement) without throwing away progress.
pub(super) fn retain_after_retirement_message(
    missing_before: usize,
    retirement: UnbackableRetirement,
    unbackable_before: usize,
    missing_after: usize,
) -> String {
    format!(
        "backup snapshot cannot complete ({missing_after} chunks not in cloud, down from \
         {missing_before} after retiring {retired} CAS-proven unbackable atom digest(s); \
         {unbackable_after} manifest chunk(s) still have no local candidate, down from \
         {unbackable_before}) — held cut retained (automatic abandon suppressed); \
         next cycle retries CAS on the same narrowed generation",
        retired = retirement.retired,
        unbackable_after = retirement.unbackable_after,
    )
}

/// Operator-facing reason a held cut is retained when the first missing set
/// produced no CAS-proven ghosts to retire.
///
/// Same retain contract as [`retain_after_retirement_message`]: do not abandon
/// and re-cut, or the next generation re-imports ghosts from the last
/// published manifest.
pub(super) fn retain_no_ghosts_message(
    missing: usize,
    unbackable_manifest_chunks: usize,
) -> String {
    format!(
        "backup snapshot cannot complete ({missing} chunks not in cloud; \
         this cut names {unbackable_manifest_chunks} manifest chunk(s) with \
         no local candidate to upload them from — carried forward from an \
         earlier manifest and since resealed away); held cut retained \
         (automatic abandon suppressed); next cycle retries CAS on the \
         same generation"
    )
}

/// Aggregate retirement across one or more passes for the retain message.
pub(super) fn fold_unbackable_retirements(passes: &[UnbackableRetirement]) -> UnbackableRetirement {
    UnbackableRetirement {
        retired: passes.iter().map(|p| p.retired).sum(),
        unbackable_after: passes.last().map_or(0, |p| p.unbackable_after),
    }
}

/// Candidate digests for [`unbackable_manifest_chunk_count`].
pub(super) fn candidate_sha_set(
    candidates: &[BackupChunkUploadCandidate],
) -> std::collections::BTreeSet<String> {
    candidates
        .iter()
        .map(|candidate| candidate.chunk.sha256.clone())
        .collect()
}

/// Extract chunk sha256s from a `backup/chunks/` cloud listing. Keys come
/// back scope-relative; anything that is not exactly a 64-hex leaf under the
/// prefix (manifests, stray objects, every `backup/v2/` key) is ignored.
/// This set seeds the presence cache and is the universe the orphan
/// selector subtracts the keep set from, so a v2 instance must never land
/// here even when its id happens to be 64-hex.
pub(super) fn shas_from_backup_chunk_listing(
    listed: &[crate::sync::auth::S3ObjectInfo],
) -> std::collections::HashSet<String> {
    listed
        .iter()
        .filter_map(|object| super::super::backup_keys::v1_chunk_sha(&object.key))
        .map(str::to_string)
        .collect()
}

/// Map a cycle's `publish_phase` onto the `'static` label `/api/status` reports.
///
/// Every phase the cycle can actually reach must be listed. A phase that falls
/// through to `cycle` is invisible to an operator: `draining`, `publishing` and
/// `failing` all used to land there, which is part of why a failing backup was
/// indistinguishable from a working one on status.
pub(super) fn progress_phase_for(publish_phase: &str) -> &'static str {
    match publish_phase {
        "published" => "published",
        "publishing" => "publishing",
        "draining" => "draining",
        "failing" => "failing",
        "chunks_only" => "chunks_only",
        "idle" => "idle",
        "building" => "building",
        _ => "cycle",
    }
}
