//! A **log-safe** view of a [`HashRangeFilter`]: the variant and its shape,
//! with every key string replaced by its [`observability::redact_id!`] token.
//!
//! Use this instead of `{:?}` in any `tracing` macro. `Debug` stays faithful
//! — it is the right thing for a debugger, a test failure, or an assertion —
//! but a filter is built from *user key material*, and a log line is not the
//! place for it: `HashKey("meeting-notes-with-alice")` is the user's data,
//! and on this node it lands in a plaintext file beside a database that is
//! encrypted at rest.
//!
//! Redaction is `redact_id!`, not `redact!`, on purpose. The keys are what
//! makes these lines worth having — you need to see that the same key was
//! queried twice, or that a scan repeated across pages — and an xxhash token
//! keeps that correlation without the content. Counts, offsets and limits
//! stay in the clear: they carry no user content and they are the numbers an
//! operator actually reads.
//!
//! Logging is core behavior. The model filter type has no log view.

use crate::schema::types::field::HashRangeFilter;

/// Log-safe [`std::fmt::Display`] wrapper for a [`HashRangeFilter`].
/// Deliberately has no `Debug`-style escape hatch back to the raw key.
pub(crate) struct RedactedHashRangeFilter<'a>(pub(crate) &'a HashRangeFilter);

impl std::fmt::Display for RedactedHashRangeFilter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // One helper so a new variant cannot accidentally format a key raw:
        // every string field below goes through it.
        fn id(s: &str) -> String {
            observability::redact_id!(s)
        }
        match self.0 {
            HashRangeFilter::HashRangeKey { hash, range } => {
                write!(
                    f,
                    "HashRangeKey {{ hash: {}, range: {} }}",
                    id(hash),
                    id(range)
                )
            }
            HashRangeFilter::HashKey(h) => write!(f, "HashKey({})", id(h)),
            HashRangeFilter::RangeKey(r) => write!(f, "RangeKey({})", id(r)),
            HashRangeFilter::HashRangePrefix { hash, prefix } => write!(
                f,
                "HashRangePrefix {{ hash: {}, prefix: {} }}",
                id(hash),
                id(prefix)
            ),
            HashRangeFilter::RangePrefix(p) => write!(f, "RangePrefix({})", id(p)),
            HashRangeFilter::HashRangeRange { hash, start, end } => write!(
                f,
                "HashRangeRange {{ hash: {}, start: {}, end: {} }}",
                id(hash),
                id(start),
                id(end)
            ),
            HashRangeFilter::RangeRange { start, end } => {
                write!(f, "RangeRange {{ start: {}, end: {} }}", id(start), id(end))
            }
            // No key material — the number is the whole point of the line.
            HashRangeFilter::SampleN(n) => write!(f, "SampleN({n})"),
            HashRangeFilter::Page { offset, limit } => {
                write!(f, "Page {{ offset: {offset}, limit: {limit} }}")
            }
            // `after` is a cursor built from a real key, so it redacts; `limit`
            // does not.
            HashRangeFilter::PageAfter { after, limit } => write!(
                f,
                "PageAfter {{ after: {}, limit: {limit} }}",
                id(&after.to_string())
            ),
            // Only the count: a per-pair list would be a key dump by another
            // name, and the length is what tells you the fan-out.
            HashRangeFilter::HashRangeKeys(pairs) => {
                write!(f, "HashRangeKeys(len={})", pairs.len())
            }
            HashRangeFilter::HashRangePattern { hash, pattern } => write!(
                f,
                "HashRangePattern {{ hash: {}, pattern: {} }}",
                id(hash),
                id(pattern)
            ),
            HashRangeFilter::RangePattern(p) => write!(f, "RangePattern({})", id(p)),
            HashRangeFilter::HashPattern(p) => write!(f, "HashPattern({})", id(p)),
            HashRangeFilter::HashRange { start, end } => {
                write!(f, "HashRange {{ start: {}, end: {} }}", id(start), id(end))
            }
        }
    }
}
