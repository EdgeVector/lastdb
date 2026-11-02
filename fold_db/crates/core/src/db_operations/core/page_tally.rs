use crate::schema::types::KeyValue;
use std::cell::{Cell, RefCell};

/// How the page keys of one query read: the count of keys it emitted in
/// verified API form and in storage form. The `/api/query` reply turns them
/// into its `key_form` flag.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageKeyForms {
    /// Page keys this query emitted in **verified API form** — a caller can
    /// point-read them back.
    pub keys_api_form: u64,
    /// Page keys this query emitted in **storage form** — under a blinding
    /// codec that is a one-way partition token, so a keyed read at it resolves
    /// nothing.
    pub keys_opaque_form: u64,
}

impl PageKeyForms {
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
/// The defect this exists to end: every hash is blinded (`blind_v1`), so a
/// page key that could not be recovered to API form is an HMAC token that is
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
    /// Per-query tally of the key form of each emitted page key and, for a
    /// caller that continues a short page, where the page ended.
    ///
    /// A node-lifetime counter would be shared, so a before/after delta around
    /// one query would pick up the counts of every other query running
    /// concurrently. This is task-scoped, and the read path is a plain
    /// `.await` chain with no `tokio::spawn` between the handler and the
    /// record read, so the tally follows the one request that installed it.
    ///
    /// **One task-local carrying both parts, deliberately.** Two nested
    /// `scope()` calls wrap the read future in one more layer of generic future
    /// type, and that layer propagates through the whole `.await` chain: it blew
    /// rustc's type-layout recursion limit in `lastdb_node`'s integration tests
    /// ("queries overflow the depth limit… query depth increased by 130 when
    /// computing layout of {async fn body of watch_target()}") while every
    /// `-p` build of the touched crates stayed green. A second part is not
    /// worth a second scope.
    static QUERY_TALLY: QueryTally;
}

/// The per-query tally: the page key forms and, for a caller that continues
/// a short page, where the page ended.
struct QueryTally {
    forms: Cell<PageKeyForms>,
    end: RefCell<PageEnd>,
    /// The caller continues a page that ends early (`with_query_page_tally`).
    resumable: bool,
}

impl QueryTally {
    fn new(resumable: bool) -> Self {
        Self {
            forms: Cell::new(PageKeyForms::default()),
            end: RefCell::new(PageEnd::default()),
            resumable,
        }
    }
}

/// Where one page ended, for the caller that builds its cursor.
///
/// One request may examine at most 10,000 keys, and a deleted record is a key.
/// A run of deleted records longer than that would make a page that must pass
/// it fail at the same place on every retry. A read that the caller can
/// continue ends its page early instead, with fewer rows than the window, and
/// names the last key it examined; the next page starts after that key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageEnd {
    /// The page ended before its window was full while keys remained.
    pub resume: Option<PageResume>,
    /// The record key position of each row of an unfiltered record walk, in
    /// record key order: the reply keeps the first rows of the walk and its
    /// cursor names the position of the last row it keeps.
    pub walk: Vec<(KeyValue, String)>,
}

/// The position after which the next page of a read starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageResume {
    /// A range under one hash: the API key of the last key the read examined.
    Key(KeyValue),
    /// An unfiltered record walk: the record key suffix of the last record
    /// the walk examined. A deleted record has no API key.
    Record(String),
}

/// True when the caller installed a tally that continues a page that ends
/// early. Another caller (a count, a condition pass, a background job) needs
/// the complete set, so a read that cannot finish in one request fails.
pub fn page_can_resume() -> bool {
    QUERY_TALLY
        .try_with(|tally| tally.resumable)
        .unwrap_or(false)
}

/// Note where a page that ended early goes on, if a tally is installed.
pub fn note_page_resume(resume: PageResume) {
    let _ = QUERY_TALLY.try_with(|tally| tally.end.borrow_mut().resume = Some(resume));
}

/// Note the record key position of each row of a record walk page.
pub fn note_walk_positions(rows: Vec<(KeyValue, String)>) {
    let _ = QUERY_TALLY.try_with(|tally| tally.end.borrow_mut().walk = rows);
}

/// Note how many page keys a query emitted in each key form, if a tally is
/// installed.
///
/// Called at the logical primary boundary. Keyed reads retain caller keys;
/// administrative inspection emits only verified API coordinates.
pub fn note_page_key_forms(api_form: u64, opaque_form: u64) {
    let _ = QUERY_TALLY.try_with(|tally| {
        let mut forms = tally.forms.get();
        forms.keys_api_form = forms.keys_api_form.saturating_add(api_form);
        forms.keys_opaque_form = forms.keys_opaque_form.saturating_add(opaque_form);
        tally.forms.set(forms);
    });
}

/// Run `fut` with a fresh per-query tally.
///
/// Returns the future's output plus the key form of its page keys.
pub async fn with_query_tally<F>(fut: F) -> (F::Output, PageKeyForms)
where
    F: std::future::Future,
{
    QUERY_TALLY
        .scope(QueryTally::new(false), async move {
            let out = fut.await;
            (out, QUERY_TALLY.with(|tally| tally.forms.get()))
        })
        .await
}

/// [`with_query_tally`] for a caller that continues a page that ends early:
/// also returns where the page ended ([`PageEnd`]).
pub async fn with_query_page_tally<F>(fut: F) -> (F::Output, PageKeyForms, PageEnd)
where
    F: std::future::Future,
{
    QUERY_TALLY
        .scope(QueryTally::new(true), async move {
            let out = fut.await;
            let (forms, end) = QUERY_TALLY.with(|tally| (tally.forms.get(), tally.end.take()));
            (out, forms, end)
        })
        .await
}
