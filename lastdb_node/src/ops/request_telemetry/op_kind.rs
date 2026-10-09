use super::*;

/// Operation class for ranking and rollups.
///
/// Keep long-poll / bulk side channels out of `Other` so ops triage does not
/// misread intentional waits (e.g. forge `local-watch` idle backoff) or pack
/// CAS file-blob traffic as opaque "git protocol" latency.
///
/// **This is a LOSSY projection of the route, and the loss is concentrated in
/// [`Self::Other`].** `LocalWatch` and `FileBlob` were split out for the
/// reason above; the same argument applies to what stayed behind. `Other` is
/// the fallback arm over roughly sixty `/api/db/*` admin routes — compaction,
/// GC, repair, audits, probes — and not one of them reports a schema, so on a
/// `(client, kind, schema)` key every one of them ranked as a single
/// `<client> / other / -` row. Splitting sixty more variants out of this enum
/// is the wrong repair: these labels are meant to be few, and a route added
/// without touching them would silently land in the fallback again. Carrying
/// the route as row IDENTITY is the right one — see [`OpSample::route`] and
/// [`OpAggregate::key`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpKind {
    Query,
    /// `POST /api/queries/batch`.
    QueryBatch,
    Mutation,
    MutationBatch,
    Status,
    Schema,
    Search,
    History,
    Atom,
    Deliver,
    /// `GET /api/local-watch` — intentional long-poll; duration often equals
    /// the client timeout (idle backoff), not store work.
    LocalWatch,
    /// `/api/db/file-blob`, `/api/db/put-blob-local`, `/api/db/fetch-file-blob`,
    /// `/api/db/fork-file-blob`.
    FileBlob,
    Other,
}

impl OpKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::QueryBatch => "query_batch",
            Self::Mutation => "mutation",
            Self::MutationBatch => "mutation_batch",
            Self::Status => "status",
            Self::Schema => "schema",
            Self::Search => "search",
            Self::History => "history",
            Self::Atom => "atom",
            Self::Deliver => "deliver",
            Self::LocalWatch => "local_watch",
            Self::FileBlob => "file_blob",
            Self::Other => "other",
        }
    }

    /// This kind's `duration_ms` is mostly the client's own requested wait, not
    /// work the node did.
    ///
    /// Ranking idle wait as consumption is actively misleading: on the primary
    /// a single `lastgit` long-poller booked 9.97M ms — 4.4x the #2 entry and
    /// more than everything else combined — purely by sleeping, and `Slowest
    /// recent` was ten identical 30149 ms `body=0B` rows. `CLAUDE.md` tells
    /// agents to "name the offender with `lastdb ops` before escalating";
    /// followed literally against that output, triage fingers the wrong client
    /// every time. So idle wait gets its own section, never the rankings.
    #[must_use]
    pub fn is_idle_wait(self) -> bool {
        matches!(self, Self::LocalWatch)
    }
}
