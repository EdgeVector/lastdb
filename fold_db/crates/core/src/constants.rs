/// The sole ID for the single public key used for signing validation.
pub const SINGLE_PUBLIC_KEY_ID: &str = "SYSTEM_WIDE_PUBLIC_KEY";

/// Production budget for best-effort mutation background task drains.
///
/// Canonicalization row `mutation-background-task-timeout`: this constant is the
/// ONLY permitted spelling of the 5s `wait_for_background_tasks(...)` budget.
/// **Test code is not exempt.** A bare `Duration::from_secs(5)` argument to
/// `wait_for_background_tasks` inside `#[cfg(test)]` / `tests/` counts as a
/// canonicalization regression, the same as production code. This literal
/// regressed three times (2026-08-22, 2026-08-23, 2026-08-30) because each
/// sighting sat in a test and was dismissed as "only a test assertion".
/// Import the constant instead:
/// `use fold_db::constants::MUTATION_BACKGROUND_TASK_TIMEOUT;` (or
/// `crate::constants::…` inside this crate).
pub const MUTATION_BACKGROUND_TASK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Default hard upper bound on atom field content size (serialized JSON bytes,
/// before encryption). **Effective** limit is
/// [`crate::atom::max_atom_content_bytes`] (env `LASTDB_MAX_ATOM_CONTENT_BYTES`).
/// Preference: `preference-lastdb-atom-size-hard-limit-64kib`.
/// Docs: `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md`.
pub const MAX_ATOM_CONTENT_BYTES: usize = 64 * 1024;
