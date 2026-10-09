use super::*;

/// Outcome of [`SchemaServiceState::backfill_purpose_statements`].
///
/// Every canonical schema in the registry contributes to exactly one
/// counter: it is either `updated` / `would_update`, or
/// `skipped_already_set`, or `skipped_no_descriptive_name`. The
/// counters sum to `considered` (modulo a small race window where a
/// concurrent writer can shift a record from `updated` to
/// `skipped_already_set` after the snapshot — that case still adds
/// up because the candidate is counted once in `considered` and once
/// in exactly one outcome bucket).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackfillPurposeReport {
    /// Total canonical schemas inspected.
    pub considered: usize,
    /// Schemas whose `purpose_statement` was filled in and persisted.
    pub updated: usize,
    /// Dry-run only — schemas that *would* have been updated.
    pub would_update: usize,
    /// Schemas skipped because `purpose_statement` was already set
    /// to a non-empty value. Idempotency: a second run hits this
    /// counter for every record the first run filled in.
    pub skipped_already_set: usize,
    /// Schemas skipped because they had no usable `descriptive_name`
    /// to derive a default from. `add_schema` rejects empty
    /// descriptive names, so this only fires for raw storage records
    /// that bypassed validation.
    pub skipped_no_descriptive_name: usize,
}

/// Internal carrier between the read-lock classification phase and
/// the write phase of [`SchemaServiceState::backfill_purpose_statements`].
struct BackfillCandidate {
    name: String,
    descriptive_name: String,
    chosen: String,
    source: &'static str,
}

impl SchemaServiceState {
    /// Backfill `purpose_statement` on every canonical schema where it
    /// is currently `None` or whitespace-only. Phase D of the
    /// `dual-signal-schema-canonicalization` fbrain design.
    ///
    /// Phase A defaults `purpose_statement` to `descriptive_name` at
    /// registration time for every *new* schema (see [`Self::add_schema`]),
    /// but schemas already persisted before Phase A still carry
    /// `purpose_statement = None`. This method walks the in-memory
    /// registry, picks a value for each empty record, and rewrites it
    /// via [`Self::persist_schema`] so both Sled (dev) and S3 (prod)
    /// backends end up consistent with the Phase A default.
    ///
    /// For each candidate, the chosen value is:
    /// 1. `mapping[descriptive_name]` if a curated entry hits, else
    /// 2. `descriptive_name` itself.
    ///
    /// `dry_run = true` logs every decision and returns
    /// `would_update` counts without writing.
    ///
    /// Idempotent: a second invocation finds nothing to do
    /// (`updated = 0`, `skipped_already_set` covers the records the
    /// first run filled in).
    ///
    /// Never holds the `schemas` write-lock across `.await` — the
    /// workspace lint floor (`clippy::await_holding_lock`) rejects
    /// that pattern.
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub async fn backfill_purpose_statements(
        &self,
        mapping: &HashMap<String, String>,
        dry_run: bool,
    ) -> FoldDbResult<BackfillPurposeReport> {
        let mut report = BackfillPurposeReport::default();

        let candidates: Vec<BackfillCandidate> = {
            let schemas = read_lock(&self.schemas, "schemas")?;
            report.considered = schemas.len();

            let mut candidates = Vec::new();
            for schema in schemas.values() {
                if !schema
                    .purpose_statement
                    .as_deref()
                    .is_none_or(|p| p.trim().is_empty())
                {
                    report.skipped_already_set += 1;
                    continue;
                }

                let descriptive = schema
                    .descriptive_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let Some(descriptive) = descriptive else {
                    tracing::warn!(
                        target: "schema_service::backfill",
                        schema = %schema.name,
                        "backfill: skipping schema with no descriptive_name — cannot derive default",
                    );
                    report.skipped_no_descriptive_name += 1;
                    continue;
                };

                let (chosen, source) = match mapping.get(descriptive) {
                    Some(curated) => (curated.clone(), "mapped"),
                    None => (descriptive.to_string(), "default_to_descriptive_name"),
                };

                candidates.push(BackfillCandidate {
                    name: schema.name.clone(),
                    descriptive_name: descriptive.to_string(),
                    chosen,
                    source,
                });
            }
            candidates
        };

        for candidate in candidates {
            let BackfillCandidate {
                name,
                descriptive_name,
                chosen,
                source,
            } = candidate;

            if dry_run {
                tracing::info!(
                    target: "schema_service::backfill",
                    schema = %name,
                    descriptive_name = %descriptive_name,
                    source = source,
                    would_set = %chosen,
                    "backfill: dry-run — would set purpose_statement",
                );
                report.would_update += 1;
                continue;
            }

            let updated_schema = {
                let mut schemas = write_lock(&self.schemas, "schemas")?;
                let Some(schema) = schemas.get_mut(&name) else {
                    tracing::warn!(
                        target: "schema_service::backfill",
                        schema = %name,
                        "backfill: schema vanished between snapshot and write — skipping",
                    );
                    continue;
                };
                if !schema
                    .purpose_statement
                    .as_deref()
                    .is_none_or(|p| p.trim().is_empty())
                {
                    tracing::info!(
                        target: "schema_service::backfill",
                        schema = %name,
                        "backfill: purpose_statement filled by concurrent writer — skipping",
                    );
                    report.skipped_already_set += 1;
                    continue;
                }
                schema.purpose_statement = Some(chosen.clone());
                schema.clone()
            };

            self.persist_schema(&updated_schema).await?;
            tracing::info!(
                target: "schema_service::backfill",
                schema = %name,
                descriptive_name = %descriptive_name,
                source = source,
                set = %chosen,
                "backfill: persisted purpose_statement",
            );
            report.updated += 1;
        }

        tracing::info!(
            target: "schema_service::backfill",
            considered = report.considered,
            updated = report.updated,
            would_update = report.would_update,
            skipped_already_set = report.skipped_already_set,
            skipped_no_descriptive_name = report.skipped_no_descriptive_name,
            dry_run = dry_run,
            "backfill: complete",
        );

        Ok(report)
    }
}
