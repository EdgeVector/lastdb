//! Conservative process memory admission and bounded write reservations.
//!
//! The native engine uses this module's deferred byte/count allowance for
//! complete write requests. Owners retain their reservation while captured
//! reads still hold the values, including after flush. If admission refuses,
//! the request stays private and completes on disk before success.
//!
//! The boot projection retains historical warm/key/graph configuration fields
//! as conservative headroom inputs. They are not measurements of native pools.
//! In particular, clean native atom retention has a logical-record count cap,
//! not a separate global body-byte cap. The cutover must not donate the removed
//! graph's projected headroom to new write admission without sizing that pool.
//!
//! ```text
//! charged        = warm_bytes + key_cache_bytes + resident_graph_bytes
//! projected_rss  = charged * rss_multiplier
//! headroom       = rss_limit - projected_rss          (saturating)
//! deferred_cap   = clamp(headroom * DEFER_HEADROOM_FRACTION,
//!                        FLOOR_DEFERRED_BYTES, MAX_DEFERRED_BYTES)
//!                  and never above remaining headroom
//! ```
//!
//! An explicit deferred-cap override is an operator input and can exceed the
//! derived headroom. The status surface reports that choice. The runtime
//! footprint observer can close new deferred admission after a hard overrun;
//! already admitted work retains its owners and drains normally.
//!
//! Native core metadata, typed definitions, and complete selected keys have
//! separate bounded allowances. Logical paths use the record-count cap in
//! [`crate::resident::RESIDENT_KEY_CAP`]. Their actual counters remain separate
//! from this projection. Neither this formula nor a single pool cap guarantees
//! an upper bound on whole-process memory.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;

use crate::resident::ResidentPolicy;

mod consts;
mod deferred_gauge;
mod env_parse;
mod footprint;
mod malloc;
mod pressure;
mod process_budget;

pub use consts::*;
pub use deferred_gauge::*;
pub use env_parse::*;
pub use footprint::*;
pub use malloc::*;
pub use pressure::*;
pub use process_budget::*;
