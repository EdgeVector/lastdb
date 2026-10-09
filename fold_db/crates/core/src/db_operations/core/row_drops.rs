/// What one query dropped between "the index says this row exists" and "the
/// caller was handed this row".
///
/// Both members are page slots the caller paid for and did not receive, so both
/// have to be added back before `has_more` is decided — see `page_payload` /
/// `cursor_payload` in `lastdb_host`.
///
/// `tombstoned` exists because the key index and the row materializer disagree
/// about what a deleted row is. `count_rows` gates on `KeyMetadata.tombstoned`;
/// the read additionally drops a row whose *body* is a tombstone value, and that
/// second gate runs after the window is taken. Such a row is counted by
/// `total_count`, consumes a page slot, and is not `unresolved` (its atom
/// resolved fine) — so before this it was invisible to both sides. Measured on
/// the primary's `Papercut` partition 2026-08-09: `total_count` 1037, distinct
/// rows reachable 606, `unresolved_rows` 0.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryRowDrops {
    /// Rows whose tip pointed at an atom body that could not be resolved.
    pub unresolved: u64,
    /// Rows whose atom body is a content tombstone.
    pub tombstoned: u64,
    /// Page keys this query emitted in **verified API form** — a caller can
    /// point-read them back. NOT a drop: see the key-form note below.
    pub keys_api_form: u64,
    /// Page keys this query emitted in **storage form** — under a blinding
    /// codec that is a one-way partition token, so a keyed read at it resolves
    /// nothing. NOT a drop: see the key-form note below.
    pub keys_opaque_form: u64,
}

impl QueryRowDrops {
    /// Page slots consumed but not delivered.
    ///
    /// Deliberately only the two DROP members. The key-form counters ride the
    /// same tally (one task-local, see below) but they count rows the caller
    /// *did* receive, so adding them here would inflate `has_more` and re-serve
    /// every page — the exact non-terminating drain `cursor_payload` documents.
    #[must_use]
    pub fn total(self) -> u64 {
        self.unresolved.saturating_add(self.tombstoned)
    }

    /// How to read the `key.hash` values this query emitted.
    ///
    /// `None` when the query emitted no page keys at all (a keyed fast-path
    /// read, or an empty result) — the caller has nothing to classify.
    #[must_use]
    pub fn key_form(self) -> Option<KeyForm> {
        match (self.keys_api_form, self.keys_opaque_form) {
            (0, 0) => None,
            (_, 0) => Some(KeyForm::Api),
            (0, _) => Some(KeyForm::Opaque),
            _ => Some(KeyForm::Mixed),
        }
    }
}

/// Whether the `key.hash` values in a page can be fed back as a key.
///
/// The defect this exists to end: under
/// [`HashKeyEncoding::BlindV1`](crate::atom::HashKeyEncoding::BlindV1) a page
/// key that could not be recovered to API form is an HMAC token that is
/// *shape-indistinguishable* from a real plaintext key. A caller listing rows
/// and then point-reading each one gets zero rows back and reads that as
/// "the data is missing" rather than "you asked with the wrong key".
///
/// Measured 2026-08-06 on the live primary: `Page{0,2} fields=["oid"]` returned
/// `key.hash = "aymNiZLjUChyji2SSJd8Wg"` where `fields=["repo"]` (the hash
/// field) returned `"schema-infra"`, with an identical `key.range`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyForm {
    /// Every emitted key is addressable — point-read it directly.
    Api,
    /// Every emitted key is a storage-form token. Do NOT use it as a key;
    /// re-read the partition projecting the schema's hash field to get
    /// addressable keys.
    Opaque,
    /// Some rows recovered and some did not. Treat any key from this page as
    /// unverified until a keyed read confirms it.
    Mixed,
}

impl KeyForm {
    /// The wire token for the `/api/query` `key_form` field.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Opaque => "opaque",
            Self::Mixed => "mixed",
        }
    }
}

tokio::task_local! {
    /// Per-query tally of rows the index counted and the caller never received.
    ///
    /// The node-lifetime counter on `DbOperations` is shared, so a
    /// before/after delta around one query would pick up skips from every
    /// other query running concurrently — on a node with 28 UDS workers that
    /// reports dangling edges against healthy requests. This is task-scoped,
    /// and the read path is a plain `.await` chain with no `tokio::spawn`
    /// between the handler and atom hydration, so the tally follows the one
    /// request that installed it.
    ///
    /// **One task-local carrying both counters, deliberately.** Two nested
    /// `scope()` calls wrap the read future in one more layer of generic future
    /// type, and that layer propagates through the whole `.await` chain: it blew
    /// rustc's type-layout recursion limit in `lastdb_node`'s integration tests
    /// ("queries overflow the depth limit… query depth increased by 130 when
    /// computing layout of {async fn body of watch_target()}") while every
    /// `-p` build of the touched crates stayed green. A second counter is not
    /// worth a second scope.
    static QUERY_ROW_DROPS: std::cell::Cell<QueryRowDrops>;
}

/// Bump one field of the per-query tally, if one is installed.
pub(super) fn note_row_drop(bump: impl Fn(&mut QueryRowDrops)) {
    let _ = QUERY_ROW_DROPS.try_with(|tally| {
        let mut drops = tally.get();
        bump(&mut drops);
        tally.set(drops);
    });
}

/// Note one row dropped for a content tombstone, if a tally is installed.
///
/// Free function rather than a `DbOperations` method: the drop happens in the
/// field read path, which holds the resolved values but not always the handle.
pub fn note_tombstoned_row() {
    note_row_drop(|drops| drops.tombstoned = drops.tombstoned.saturating_add(1));
}

/// Note how many page keys a query emitted in each key form, if a tally is
/// installed.
///
/// Called once per resolved page at the single boundary that holds the
/// storage → API key mapping
/// (`HashRangeQueryProcessor::rename_page_to_api_keys`), because that is the
/// only place that knows which form escaped to the caller.
pub fn note_page_key_forms(api_form: u64, opaque_form: u64) {
    note_row_drop(|drops| {
        drops.keys_api_form = drops.keys_api_form.saturating_add(api_form);
        drops.keys_opaque_form = drops.keys_opaque_form.saturating_add(opaque_form);
    });
}

/// Run `fut` with a fresh per-query row-drop tally.
///
/// Returns the future's output plus the rows *this* query dropped after the
/// index had counted them: unresolvable tip → atom edges, and content
/// tombstones the window had already spent a slot on.
pub async fn with_query_row_drop_tally<F>(fut: F) -> (F::Output, QueryRowDrops)
where
    F: std::future::Future,
{
    QUERY_ROW_DROPS
        .scope(std::cell::Cell::new(QueryRowDrops::default()), async move {
            let out = fut.await;
            (out, QUERY_ROW_DROPS.with(std::cell::Cell::get))
        })
        .await
}
