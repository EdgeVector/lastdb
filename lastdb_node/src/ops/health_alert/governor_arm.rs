//! The fifth degradation arm `probe_degradation` reads: a `governor_state`
//! of `purge-failed` that has HELD past [`GOVERNOR_PURGE_FAILED_WARN_SECS`].
//!
//! **Why this arm has its own duration gate, where the others do not.**
//! `purge-failed` is not latched the way `runtime_degraded` and
//! `footprint_over_limit` are: it is reassigned on every tick from whether
//! the LATEST purge succeeded (`footprint.rs`'s `set_governor_state` /
//! `score_purge`), so a single bursty tick can read `purge-failed` on an
//! otherwise healthy node and clear itself on the next tick. Measured on the
//! primary 2026-10-07
//! (papercut-lastdb-governor-state-latched-without-an-age-and-masks-six-states-20261007):
//! the SAME `purge-failed` reading held for 13h12m, then still held past
//! 46h, with no instrument anywhere reporting the age — and
//! `lastdb alert-check` itself said nothing about any of it, in the same
//! minute, on the same node. That record's fix stamped the age
//! (`governor_state_held_secs`) so `lastdb status` could tell a blip from a
//! latch; this arm is the alarm that was still missing after that fix
//! shipped. Gating on the age is what keeps a one-tick blip quiet while
//! still catching the multi-hour latch this arm exists to catch.
//!
//! Not caller-configurable (unlike `--slowest-request-warn-ms`): a latency
//! budget is something different routines legitimately disagree about, a
//! multi-hour memory-governor latch is not.

/// How long `governor_state` must have held `purge-failed` before
/// [`stale_purge_failed_reason`] reports it.
pub(super) const GOVERNOR_PURGE_FAILED_WARN_SECS: u64 = 3_600;

/// `purge-failed` is the governor's highest-precedence state (it masks the
/// other six while it holds), so it is the one reading most likely to be
/// mistaken for news. Gate on the age, not the bare bool — see the module
/// doc for why. `governor_state_held_secs` is absent on an older daemon
/// payload (pre-`governor_state_since_epoch_secs`); absence reads as "below
/// budget", same as a missing `slowest_request_ms` would.
pub(super) fn stale_purge_failed_reason(mem: &serde_json::Value) -> Option<String> {
    if mem
        .get("governor_state")
        .and_then(serde_json::Value::as_str)
        != Some("purge-failed")
    {
        return None;
    }
    let held = mem
        .get("governor_state_held_secs")
        .and_then(serde_json::Value::as_u64)?;
    if held < GOVERNOR_PURGE_FAILED_WARN_SECS {
        return None;
    }
    Some(format!(
        "memory_budget.governor_state=purge-failed held {held}s (budget {GOVERNOR_PURGE_FAILED_WARN_SECS}s)"
    ))
}
