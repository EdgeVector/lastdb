//! Resident graph — T0 primary working set (memory → disk → cloud).
//!
//! Product law: `concepts-lastdb-rehydrate`, `docs/lastdb-rehydrate.md`.
//!
//! ## Fidelity
//!
//! What is in resident memory is **exactly** what will be written later.
//! [`ResidentGraph::dirty_persist_plan`] is an isomorphic projection of dirty
//! ladder objects (schema → molecule tip → atom). Persist must
//! write that plan without rewriting content.
//!
//! ## Ladder
//!
//! ```text
//! schema → field → molecule (tips) → atom (file reference only)
//! protein binds molecules
//! ```
//! CAS bytes are outside this graph. Query results carry file access metadata;
//! explicit file access owns its transient buffers and disk storage.
//!
//! ## Ops
//!
//! - **`resolve` / `rehydrate`** — read miss path
//! - **`apply`** — mutate resident + dirty pin
//! - **`dirty_persist_plan` / mark persisted** — T1 drain surface
//! - **`fold`** — protein tip fan-out (`crate::protein`; not rehydrate)
//!
//! ## Logical resident set
//!
//! [`logical_set::LogicalResidentSet`] keeps one hold-counted entry per
//! logical key. A call takes a hold on admit and releases it on return. A
//! release keeps a clean record warm. A dirty entry stores a
//! [`logical_set::DurabilityToken`]. After that token is covered and the
//! owning call has returned, the entry stays warm. The set does not install
//! into [`ResidentGraph`]. This slice does not read
//! `LASTDB_LOGICAL_RESIDENT_SET`. [`logical_set::RESIDENT_KEY_CAP`] caps
//! used records. A fetch marks that record most recent. Recency is ordered
//! by tick. Over the cap, LRU purge removes the cold end. It skips a held
//! record and a dirty record without touching the tick.
//! Point-get admission marks each fetched record most recent. Range fill
//! holds each admitted tip for the call and keeps those records warm after
//! it releases the holds.

mod config;
mod graph;
mod lane;
mod ledger;
mod logical_set;
mod metrics;
mod persist;
mod point;
mod range;
mod types;
mod worker;

pub use config::{
    parse_resident_bytes, parse_resident_max_deferred, parse_resident_mode,
    parse_resident_persist_interval, ResidentMode, ResidentPolicy, DEFAULT_RESIDENT_BYTES,
    DEFAULT_RESIDENT_MAX_DEFERRED, DEFAULT_RESIDENT_PERSIST_MS, RESIDENT_BYTES_ENV,
    RESIDENT_MAX_DEFERRED_ENV, RESIDENT_MODE_ENV, RESIDENT_PERSIST_MS_ENV,
};
pub(crate) use graph::SlotRead;
pub use graph::{MapSchemaLoader, ResidentGraph, SchemaLoader};
pub use lane::{
    PersistBatchOutcome, PersistEnvelope, PersistLaneFull, PersistLaneKey, PersistLaneOccupancy,
    PersistLanePressure, PersistLaneSet, PersistLaneWriter, PersistReservation,
    PersistSlotRevision,
};
pub use logical_set::{
    init_resident_key_cap_from_env, parse_resident_key_cap, resident_key_cap, AtomId,
    DurabilityToken, FieldEntry, HashCompleteness, LogicalResidentSet, MoleculeId,
    RangeNotResident, ResidentKey, Tip, RESIDENT_KEY_CAP, RESIDENT_KEY_CAP_ENV,
};
pub use metrics::{ResidentMetrics, ResidentMetricsSnapshot};
pub use persist::{AtomStorePersistSink, PersistSink, RehydrateSource};
pub use point::{PointAdmitError, PointGetOutcome, PointLoader};
pub use range::{HashLoader, RangeAdmitError, RangePage};
pub use types::{
    DirtyKey, PersistPlan, ResidentAtom, ResidentKeySetCompleteness, ResidentKeySetSnapshot,
    ResidentKind, ResidentMoleculeKey, ResidentSlotControl, ResidentSlotId, ResidentSlotState,
    ResidentTip, ResolveOutcome, ResolveSource, RESIDENT_KIND_ATOM, RESIDENT_KIND_FILE_BLOB,
    RESIDENT_KIND_MOLECULE, RESIDENT_KIND_MOLECULE_TIP, RESIDENT_KIND_PROTEIN,
    RESIDENT_KIND_SCHEMA,
};
pub use worker::BackgroundPersistTask;
