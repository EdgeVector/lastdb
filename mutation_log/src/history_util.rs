//! Small helpers re-exported from the history module.

/// Compute expected next lamport for authoring.
#[must_use]
pub fn next_lamport(prev_lamport: u64, seen_lamports: impl Iterator<Item = u64>) -> u64 {
    let mut max = prev_lamport;
    for l in seen_lamports {
        max = max.max(l);
    }
    1 + max
}
