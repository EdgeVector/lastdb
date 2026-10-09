//! One blessed way to attach context to a fallible operation.
//!
//! ## Why this exists
//!
//! Across the workspace, low-level errors (`StorageError`, `serde_json::Error`,
//! lock-poison, I/O, …) get hand-mapped into a domain error with a
//! human-readable prefix:
//!
//! ```ignore
//! store.put(k, v).await.map_err(|e| FoldDbError::Database(format!("insert consent: {e}")))?;
//! ```
//!
//! Repeated enough, every module grew its own private wrapper — `kv_err`,
//! `cache_err`, historical `sled_err`, … — each reinventing the same shape but
//! bound to a different target error type, so none of them could be shared.
//! That is why the same "collapse N map_err closures into a helper" refactor
//! kept landing module by module.
//!
//! This module ends that whack-a-mole with two idioms, so there is exactly one
//! answer to "how do I map this low-level error?":
//!
//! 1. **No message to add** — implement `From<SourceError>` on your domain
//!    error and use a bare `?`.
//! 2. **A message genuinely helps** — `.context("verb the thing")?`:
//!
//! ```ignore
//! use fold_db::error_context::ResultExt;
//!
//! store.put(k, v).await.context("insert consent")?;      // -> ConsentStoreError
//! ns.open_namespace(name).await.context("open cache")?;  // -> FoldDbError
//! store.put(k, v).await.context("save async query")?;    // -> String
//! ```
//!
//! `.context(..)` returns a concrete [`ContextError`] carrying the formatted
//! `"{context}: {source}"` string. The `?` then makes a *single* `From`
//! conversion into the caller's own error type — every domain error opts in
//! with a one-line `impl From<ContextError>`. Because the source type is
//! concrete, there is never the inference ambiguity a generic
//! `from_context::<E>()` would hit at a `?` site with many `From` impls.

use std::fmt::{self, Display};

/// The error produced by [`ResultExt::context`] — a message that has already
/// absorbed the source error's `Display` output as `"{context}: {source}"`.
///
/// It exists so `.context(..)?` converts in a *single* concrete `From` hop into
/// the caller's domain error (each target implements `From<ContextError>`),
/// rather than through a generic bound the compiler cannot resolve by impl
/// search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextError(pub String);

impl Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ContextError {}

/// Extension trait adding `.context()` / `.with_context()` to any `Result`
/// whose error implements [`Display`].
pub trait ResultExt<T> {
    /// Convert the error into a [`ContextError`], prefixing it with `context`.
    ///
    /// Follow with `?` to land in the caller's domain error (which must
    /// implement `From<ContextError>`). Use a string literal or any cheap
    /// `Display`; for a message expensive to build, prefer [`with_context`].
    ///
    /// [`with_context`]: ResultExt::with_context
    fn context(self, context: impl Display) -> Result<T, ContextError>;

    /// Like [`context`], but the message is only built on the error path.
    ///
    /// [`context`]: ResultExt::context
    fn with_context<S, F>(self, f: F) -> Result<T, ContextError>
    where
        S: Display,
        F: FnOnce() -> S;
}

impl<T, OrigErr: Display> ResultExt<T> for Result<T, OrigErr> {
    fn context(self, context: impl Display) -> Result<T, ContextError> {
        self.map_err(|e| ContextError(format!("{context}: {e}")))
    }

    fn with_context<S, F>(self, f: F) -> Result<T, ContextError>
    where
        S: Display,
        F: FnOnce() -> S,
    {
        self.map_err(|e| ContextError(format!("{}: {e}", f())))
    }
}

// --- Target conversions (one concrete `From` hop each) ------------------------

/// For call sites whose return type is `Result<_, String>` (e.g. discovery's
/// KV plumbing). Yields the formatted message verbatim.
impl From<ContextError> for String {
    fn from(c: ContextError) -> Self {
        c.0
    }
}

impl From<ContextError> for crate::error::FoldDbError {
    /// Contextualized low-level failures are overwhelmingly storage/IO-shaped,
    /// so they land in [`FoldDbError::Database`]. Nothing in the workspace
    /// matches on that variant (only `Schema`/`Permission`/
    /// `Serialization` are destructured), and the HTTP layer maps both
    /// `Database` and `Config` to 500, so the choice is behavior-neutral.
    ///
    /// [`FoldDbError::Database`]: crate::error::FoldDbError::Database
    fn from(c: ContextError) -> Self {
        Self::Database(c.0)
    }
}
