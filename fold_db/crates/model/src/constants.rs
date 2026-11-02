/// Production budget for best-effort mutation background task drains.
///
/// Canonicalization row `mutation-background-task-timeout`: this constant is the
/// ONLY permitted spelling of the 5s `wait_for_background_tasks(...)` budget.
/// A bare `Duration::from_secs(5)` argument to `wait_for_background_tasks`
/// counts as a canonicalization regression. This literal regressed three times
/// (2026-08-22, 2026-08-23, 2026-08-30). Import the constant instead:
/// `use fold_db::constants::MUTATION_BACKGROUND_TASK_TIMEOUT;` (or
/// `crate::constants::…` inside this crate).
pub const MUTATION_BACKGROUND_TASK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
