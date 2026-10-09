//! Generic schema name detection and rejection.
//!
//! Schema descriptive names must describe the *content topic* — e.g.
//! "Family Vacation Photos" or "Technical Architecture Notes" — not
//! structural terms like "Document Collection" or "Data Records".
//!
//! Used by both the ingestion pipeline (to re-prompt the AI) and the
//! schema service (to reject at registration time).

use std::collections::HashSet;

/// Words that describe structure/format rather than content.
/// A name composed entirely of these words is generic.
/// Words that describe structure, format, or container type rather than content.
/// A name composed entirely of these words is generic and will be rejected.
/// Names must have at least one content-specific word (e.g., "Family" in
/// "Family Photos" or "Concert" in "Concert Videos").
const GENERIC_WORDS: &[&str] = &[
    // Container / structure words
    "album",
    "archive",
    "catalog",
    "catalogue",
    "collection",
    "database",
    "entries",
    "entry",
    "gallery",
    "item",
    "items",
    "library",
    "list",
    "log",
    "logs",
    "metadata",
    "object",
    "objects",
    "record",
    "records",
    "repository",
    "set",
    "store",
    // Generic content-type words (describe form, not topic — "article" is a
    // shape of writing, not what the writing is about)
    "article",
    "articles",
    "blurb",
    "blurbs",
    "clip",
    "clips",
    "memo",
    "memos",
    "note",
    "notes",
    "page",
    "pages",
    "piece",
    "pieces",
    "post",
    "posts",
    "snippet",
    "snippets",
    // Format / media words (describe the file type, not the content topic)
    "audio",
    "content",
    "data",
    "document",
    "documents",
    "file",
    "files",
    "image",
    "images",
    "markdown",
    "pdf",
    "pdfs",
    "photo",
    "photograph",
    "photographs",
    "photos",
    "picture",
    "pictures",
    "text",
    "video",
    "videos",
    // Method-of-ingestion words (describe how the data got here, not what
    // it is — "Document Extractions" is the pipeline, not the records)
    "conversion",
    "conversions",
    "converted",
    "extracted",
    "extraction",
    "extractions",
    "imported",
    "imports",
    "ingested",
    "ingestion",
    "ocr",
    "parsed",
    "processed",
    "scanned",
    "uploaded",
    "uploads",
    // Filler words
    "general",
    "generic",
    "information",
    "misc",
    "miscellaneous",
    "mixed",
    "my",
    "other",
    "personal",
    "various",
    // Stop words
    "the",
    "with",
    "and",
    "of",
    "a",
    "an",
];

/// Returns `true` if the name is too generic to be useful as a schema name.
///
/// A name is generic if **every meaningful word** (after lowercasing) is in
/// the [`GENERIC_WORDS`] set. Names with at least one content-specific word
/// pass — e.g. "Medical Records" passes because "medical" is specific.
///
/// # Examples
///
/// ```
/// use schema_service_core::name_validator::is_generic_name;
///
/// assert!(is_generic_name("Document Collection"));
/// assert!(is_generic_name("Data Records"));
/// assert!(is_generic_name("text content"));
///
/// assert!(!is_generic_name("Family Vacation Photos"));
/// assert!(!is_generic_name("Medical Records"));
/// assert!(!is_generic_name("Technical Notes"));
/// ```
pub fn is_generic_name(name: &str) -> bool {
    let generic_set: HashSet<&str> = GENERIC_WORDS.iter().copied().collect();

    // Trim leading/trailing punctuation off each whitespace-separated token,
    // lowercase it, and drop tokens that disappear entirely (em dash, ellipsis,
    // stray hyphen acting as a visual separator). LLM-generated names often
    // include such tokens — "Photo — Collection", "Data … Records" — and
    // counting an empty trimmed token as "not generic" would let an otherwise
    // generic name slip through the all() check. This mirrors the empty-token
    // filter in [`is_over_specific_name`].
    let meaningful_words: Vec<String> = name
        .split_whitespace()
        .map(|w| {
            w.trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase()
        })
        .filter(|w| !w.is_empty())
        .collect();
    if meaningful_words.is_empty() {
        return true;
    }

    meaningful_words
        .iter()
        .all(|w| generic_set.contains(w.as_str()))
}

/// Returns `Err` with a descriptive message if the name is generic.
///
/// The error message is designed to be included in AI retry prompts.
pub fn reject_generic_name(name: &str) -> Result<(), String> {
    if is_generic_name(name) {
        Err(format!(
            "Schema descriptive_name '{name}' is too generic. \
             The name must describe the CONTENT TOPIC — read the actual data and name it \
             specifically (e.g., 'Family Vacation Photos', 'Technical Architecture Notes', \
             'Weekly Meeting Minutes')."
        ))
    } else {
        Ok(())
    }
}

/// Max words allowed in a descriptive_name before [`is_over_specific_name`]
/// flags it as instance-level rather than category-level.
///
/// Smart-folder ingestion classifies one file at a time. When the LLM names
/// the schema after the file's *subject* (e.g. "Roasted Tomato Soup Recipe"
/// for a single recipe note) the schema namespace explodes to one-file-per-
/// schema, defeating structured queries. Names of category genres
/// ("Recipes", "Journal Entries", "Family Vacation Photos") fit in three
/// words or fewer in the overwhelming majority of cases; the prompt
/// instructs the classifier to stay under this cap.
pub const MAX_DESCRIPTIVE_NAME_WORDS: usize = 3;

/// Returns `true` if the name appears to be instance-level rather than
/// category-level — i.e. likely the title of a single file rather than a
/// genre that two files of the same kind could share.
///
/// The current heuristic is a hard word-count cap: names with more than
/// [`MAX_DESCRIPTIVE_NAME_WORDS`] meaningful words are treated as over-
/// specific. Empty names are not over-specific (they're caught by the
/// non-empty check upstream).
///
/// Punctuation tokens that aren't whitespace-separated count as part of
/// their adjacent word; we trim the same way [`is_generic_name`] does.
///
/// # Examples
///
/// ```
/// use schema_service_core::name_validator::is_over_specific_name;
///
/// // Instance-level — too specific:
/// assert!(is_over_specific_name("Roasted Tomato Soup Recipe"));
/// assert!(is_over_specific_name("Paris Trip 2024 Journal"));
/// assert!(is_over_specific_name("How Rust Borrow Checker Works"));
///
/// // Category-level — passes:
/// assert!(!is_over_specific_name("Recipes"));
/// assert!(!is_over_specific_name("Travel Notes"));
/// assert!(!is_over_specific_name("Family Vacation Photos"));
/// assert!(!is_over_specific_name("Customer Orders"));
/// ```
pub fn is_over_specific_name(name: &str) -> bool {
    let meaningful_word_count = name
        .split_whitespace()
        .filter(|w| !w.trim_matches(|c: char| !c.is_alphanumeric()).is_empty())
        .count();
    meaningful_word_count > MAX_DESCRIPTIVE_NAME_WORDS
}
