//! Read conflict coverage, then stamp the response with data-only rules.

use super::*;

mod rules;

/// Stamp `has_conflicts` and return the page field `conflict_flags`.
///
/// One point get of `hcu:mols`. When that stamp exists, one event range for
/// seq greater than `folded_seq`. An absent stamp is `"unknown"` and returns.
///
/// `LASTDB_HOME_CONFLICT_INDEX_DISABLE` selects bounded per-molecule reads.
/// The reads stay unprefixed; org-prefixed conflicts stay out of this index.
///
/// A format 7 home reads the conflict markers of the rows of `schema_name` by
/// key, and stamps each conflicted field.
pub(in crate::handlers) async fn annotate_conflict_flags<H: HostNode>(
    host: &H,
    schema_name: &str,
    results: &mut [Value],
) -> &'static str {
    let db = host.fold_db();
    match db.db_ops().read_home_conflict_annotation(None).await {
        HomeConflictAnnotation::Records => {
            if results.is_empty() {
                return "known";
            }
            let flags = match rules::row_keys(results) {
                Some(keys) => db.record_conflict_flags(schema_name, &keys).await,
                None => None,
            };
            rules::stamp_record_conflict_flags(results, flags.as_deref())
        }
        HomeConflictAnnotation::Disabled => {
            let molecules = rules::collect_molecule_uuids(results);
            if molecules.is_empty() {
                return "known";
            }
            let concurrency = rules::resolve_lookup_concurrency(
                std::env::var("LASTDB_CONFLICT_LOOKUP_CONCURRENCY")
                    .ok()
                    .as_deref(),
            );
            let flagged: HashSet<String> = futures::stream::iter(molecules)
                .map(|mol| async move {
                    db.db_ops()
                        .get_unresolved_conflicts(Some(&mol), None)
                        .await
                        .is_ok_and(|cs| !cs.is_empty())
                        .then_some(mol)
                })
                .buffer_unordered(concurrency)
                .filter_map(futures::future::ready)
                .collect()
                .await;
            // Preserve the legacy fallback: a failed lookup adds no flag.
            rules::stamp_conflict_flags(results, &flagged, true)
        }
        HomeConflictAnnotation::Unknown => {
            rules::stamp_conflict_flags(results, &HashSet::new(), false)
        }
        HomeConflictAnnotation::Known { molecules } => {
            rules::stamp_conflict_flags(results, &molecules, true)
        }
        HomeConflictAnnotation::Incomplete { molecules } => {
            rules::stamp_conflict_flags(results, &molecules, false)
        }
    }
}
