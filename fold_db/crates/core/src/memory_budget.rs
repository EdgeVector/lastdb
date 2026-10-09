//! One accounted process memory budget.
//!
//! # Why this module exists
//!
//! LastDB sizes memory with several independent env knobs. Each was chosen
//! alone, against its own workload, and nothing computed their sum. On
//! 2026-07-29 the primary ran 4 GiB of hash-group warm set, 64 MiB of key
//! cache, a 2 GiB resident graph, and an unbounded deferred-persist queue
//! against `LASTDBD_RSS_LIMIT_MB=12288`. It reached 10-11 GiB within four
//! minutes of every boot, took `SIGTERM` from `lastdbd-memory-guard`, and was
//! kickstarted — eight times in one hour (brain
//! `incident-primary-lastdbd-restart-loop-10-11gb-rss-after-0231-183-cutover`).
//!
//! No single knob was wrong. Their sum had no owner.
//!
//! This module gives it one: it charges every budget the process has chosen,
//! projects RSS from that charge, subtracts it from the guard limit, and hands
//! the **remaining headroom** to the one consumer that can be bounded at
//! runtime — the deferred-persist window of `LASTDB_RESIDENT_MODE=write`.
//! Raising the warm budget now visibly shrinks the defer window instead of
//! silently borrowing memory the guard will kill the node for.
//!
//! # The arithmetic
//!
//! ```text
//! charged        = warm_bytes + key_cache_bytes + resident_graph_bytes
//!                  + logical_resident_set_charged_bytes   (always 0)
//! projected_rss  = charged * rss_multiplier
//! headroom       = rss_limit - projected_rss          (saturating)
//! deferred_cap   = clamp(headroom * DEFER_HEADROOM_FRACTION,
//!                        FLOOR_DEFERRED_BYTES, MAX_DEFERRED_BYTES)
//!                  and never above remaining headroom
//! ```
//!
//! The logical resident set has no byte term. Its cap is a used-record count
//! ([`crate::resident::RESIDENT_KEY_CAP`]). This module does not add
//! `warm_bytes`, `key_cache_bytes`, the resident-graph counter, or a new
//! `resident_bytes` field for that set. Flag-off byte accounting is the
//! three-term sum above plus the zero logical term.
//!
//! 64 MiB is the **floor** when headroom can fund it, not the ceiling. The
//! ceiling is [`MAX_DEFERRED_BYTES`] (a crash-window bound). Because
//! `deferred_cap <= headroom`, `projected_rss + deferred_cap` stays under the
//! guard. When `projected_rss` alone exceeds the limit the config cannot fit:
//! the cap goes to **zero** (mode=write degrades to the inline durable path
//! for every batch) and boot logs an error. A node that cannot fit its
//! budgets stops deferring instead of killing itself.
//!
//! # Where the multiplier comes from
//!
//! The charged budgets do not predict RSS. #942 made a resident hash group
//! cheap in *budgeted* bytes, so far more groups stay resident, and each one
//! still carries per-handle structures the budget does not count — so uncounted
//! memory scales with group count, which scales with the budget. Measured on
//! the live primary the same day: warm 4.00 GiB charged, RSS settled at 6.40
//! GiB over four flat samples at 14-16 min uptime. That is the
//! [`DEFAULT_RSS_MULTIPLIER`] of 1.6. Brain:
//! `lastdb-942-cutover-node-level-proof-and-the-rss-multiplier`.
//!
//! Expressing the uncounted term as a *multiple of the charge* rather than a
//! fixed constant is deliberate: it is per-handle overhead on handles the
//! budget admits, so it scales with the budget. A fixed 2.4 GiB constant would
//! be wildly wrong for a 256 MiB-preset node.
//!
//! # Env
//!
//! | Variable | Meaning |
//! |----------|---------|
//! | `LASTDBD_RSS_LIMIT_MB` | Process memory ceiling (default 16384 = 16 GiB phys_footprint policy; must match the external kill guard). |
//! | `LASTDBD_GUARD_METRIC` | Which gauge that ceiling is enforced on: `rss` or `footprint` (default `footprint`; must match the external kill guard). |
//! | `LASTDB_HASH_GROUP_WARM_BYTES` | Hash-group warm-set body budget for non-logical collections (indexes, schema_index, atom_ref_edges_v2, keep_small, metadata, cas_blobs). Charged as-is. Does not size the logical resident set. |
//! | `LASTDB_HASH_GROUP_KEY_CACHE_BYTES` | Key-index cache budget, charged as-is. |
//! | `LASTDB_RESIDENT_BYTES` | ResidentGraph budget (`LASTDB_RESIDENT_MODE=write`), charged as-is (see [`crate::resident::config`]). Does not size the logical resident set ([`crate::resident::RESIDENT_KEY_CAP`]). |
//! | `LASTDB_RSS_BUDGET_MULTIPLIER` | Charge → RSS multiplier (default 1.6, clamped to \[1.0, 8.0\]). |
//! | `LASTDB_RESIDENT_MAX_DEFERRED_BYTES` | Explicit deferred-window cap, overriding the derived one. `0` disables deferral. |
//! | `LASTDB_DEFER_LANE_FAIR_SHARE_PERCENT` | Max percent of the deferred window one persist lane may occupy (default 50). |
//! | `LASTDB_DEFER_WRITE_THROUGH_BYTES` | Batches at or above this size persist inline (default 4 MiB). `0` disables. |
//!
//! The three budget knobs are **read**, never rewritten: #942 tuned the warm
//! budget for a measured -61% on live `kanban list`, and lowering it re-opens
//! the 2026-07-22 thrash regression. This module reports what they cost and
//! bounds the one term that was never bounded.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;

use crate::resident::ResidentPolicy;

mod consts;
mod deferred_gauge;
mod env_parse;
mod estimate;
mod footprint;
mod malloc;
mod pressure;
mod process_budget;

pub use consts::*;
pub use deferred_gauge::*;
pub use env_parse::*;
pub use estimate::*;
pub use footprint::*;
pub use malloc::*;
pub use pressure::*;
pub use process_budget::*;
