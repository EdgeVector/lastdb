//! Read conflict coverage, then stamp the response with data-only rules.

use super::*;

mod rules;

/// Stamp `has_conflicts` and return the page field `conflict_flags`.
///
/// A conflict belongs to its record: a marker beside each record key names
/// the conflicted fields. This reads the markers of the rows of
/// `schema_name` by key and stamps each conflicted field. A page whose rows
/// carry no key, or a failed read, stamps every field unknown, not clean.
pub(in crate::handlers) async fn annotate_conflict_flags<H: HostNode>(
    host: &H,
    schema_name: &str,
    results: &mut [Value],
) -> &'static str {
    if results.is_empty() {
        return "known";
    }
    let flags = match rules::row_keys(results) {
        Some(keys) => {
            host.fold_db()
                .record_conflict_flags(schema_name, &keys)
                .await
        }
        None => None,
    };
    rules::stamp_record_conflict_flags(results, flags.as_deref())
}
