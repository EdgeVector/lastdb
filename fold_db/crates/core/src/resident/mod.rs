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
//! [`ResidentMetrics`] holds shared process counters. Typed values, logical
//! keys, and retained read versions belong only to the core model.
//!
//! ## Shared logical contract
//!
//! ```text
//! schema → field → molecule (tips) → atom (file reference only)
//! protein binds molecules
//! ```
//!
//! [`RESIDENT_KEY_CAP`] limits used logical records. A call holds its selected
//! paths. The purge skips held records and writes that still await flush.
//! CAS file bytes stay outside the logical model. Queries return file
//! references and access metadata rather than retaining file contents.
//! See `docs/lastdb-rehydrate.md` for the explicit backend boundary.

mod config;
pub(crate) mod core_keys;
mod core_set;
mod key_cap;
mod lane;
mod metrics;
mod types;

pub use config::{
    parse_resident_bytes, parse_resident_max_deferred, parse_resident_persist_interval,
    ResidentPolicy, DEFAULT_RESIDENT_BYTES, DEFAULT_RESIDENT_MAX_DEFERRED,
    DEFAULT_RESIDENT_PERSIST_MS, RESIDENT_BYTES_ENV, RESIDENT_MAX_DEFERRED_ENV,
    RESIDENT_PERSIST_MS_ENV,
};
pub(crate) use core_set::CoreMemorySource;
pub use core_set::{CoreMemoryStats, CoreMemoryValue};
pub(crate) use core_set::{
    CoreModelChange, CoreModelSnapshot, CoreModelStats, CoreModelUpdate, PreparedCoreMemory,
};
pub(crate) use core_set::{CorePath, CoreWarmLookup, CoreWarmSet, CoreWarmSlot, CoreWarmTip};
pub use key_cap::{
    init_resident_key_cap_from_env, parse_resident_key_cap, resident_key_cap, RESIDENT_KEY_CAP,
    RESIDENT_KEY_CAP_ENV,
};
pub use lane::{
    PersistBatchOutcome, PersistEnvelope, PersistLaneFull, PersistLaneKey, PersistLaneOccupancy,
    PersistLanePressure, PersistLaneSet, PersistLaneWriter, PersistReservation,
    PersistSlotRevision,
};
pub use metrics::{ResidentMetrics, ResidentMetricsSnapshot};
pub use types::{
    ResidentKind, RESIDENT_KIND_ATOM, RESIDENT_KIND_FILE_BLOB, RESIDENT_KIND_MOLECULE,
    RESIDENT_KIND_PROTEIN, RESIDENT_KIND_SCHEMA,
};
