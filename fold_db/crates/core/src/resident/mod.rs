//! Core logical memory and shared process counters.
//!
//! [`CoreWarmSet`] owns schemas, molecule definitions, complete selected keys,
//! sparse current tips, confirmed absence, and full atom objects. A call
//! captures typed dirty state with its matching disk root. Held owners keep
//! earlier values charged; the separate path order controls clean retention.
//!
//! Native writes reserve one request and publish typed core effects. The
//! private disk queue owns prepared output and flush work. Ordinary reads
//! cannot inspect it. Unsupported raw controls finish on disk before their
//! invalidation becomes visible.
//!
//! [`PersistLaneMetrics`] holds background purge counters. Typed values, logical
//! keys, and retained read versions belong only to the core model.
//!
//! ## Shared logical contract
//!
//! ```text
//! schema → field → molecule (tips) → atom (file reference only)
//! protein binds molecules
//! ```
//!
//! The warm set is limited by charged bytes ([`core_warm_bytes`]). A call holds
//! its selected paths. The purge skips held records and writes that still
//! await flush; when only those remain, a new record enters over the limit.
//! CAS file bytes stay outside the logical model. Queries return file
//! references and access metadata rather than retaining file contents.
//! See `docs/lastdb-rehydrate.md` for the explicit backend boundary.

mod config;
pub(crate) mod core_keys;
mod core_set;
mod lane;
mod metrics;
mod warm_bytes;

pub use config::{
    parse_resident_max_deferred, ResidentPolicy, DEFAULT_RESIDENT_MAX_DEFERRED,
    RESIDENT_MAX_DEFERRED_ENV,
};
pub(crate) use core_set::CoreMemorySource;
pub use core_set::{CoreMemoryStats, CoreMemoryValue, CoreWarmByteStats};
pub(crate) use core_set::{
    CoreModelChange, CoreModelSnapshot, CoreModelStats, CoreModelUpdate, PreparedCoreMemory,
};
pub(crate) use core_set::{CorePath, CoreWarmLookup, CoreWarmSet, CoreWarmSlot, CoreWarmTip};
pub use lane::{
    PersistBatchOutcome, PersistEnvelope, PersistLaneFull, PersistLaneKey, PersistLaneOccupancy,
    PersistLanePressure, PersistLaneSet, PersistLaneWriter, PersistReservation,
};
pub use metrics::{PersistLaneMetrics, PersistLaneMetricsSnapshot};
pub use warm_bytes::{
    core_warm_bytes, core_warm_dirty_bytes, init_core_warm_bytes_from_env, parse_core_warm_bytes,
    CORE_WARM_BYTES_ENV, CORE_WARM_DIRTY_PERCENT, DEFAULT_CORE_WARM_BYTES, RETIRED_KEY_CAP_ENV,
};
