//! Resident graph — T0 primary working set (memory → disk → cloud).
//!
//! Product law: `concepts-lastdb-rehydrate`, `docs/lastdb-rehydrate.md`.
//!
//! ## Load-bearing: do not remove, merge, or "clean up" as unused
//!
//! This module holds the live memory and write owners. `LASTDB_RESIDENT_MODE=off` (the env
//! default) does **not** mean the code is dead or experimental: the live node
//! has run `write` mode (see `lastdb_node` restore `replay_tail`, which forces
//! `off` only because a one-shot CLI has no persist worker).
//!
//! - [`ResidentGraph`] (`graph*`, `coverage`, `types`) is built by every
//!   `DbOperations`, in every mode. Native storage and `DbOperations` share
//!   this owner. Native logical values live in the shared core model. Its legacy per-slot
//!   revisions are the mutation
//!   concurrency gate (`mutation_manager/molecules/apply_gate.rs`), and
//!   protein sibling rebase reads tips from it. The mode only decides whether
//!   reads go resident-first (`read`, `write`) and whether a mutation is
//!   acknowledged on resident apply (`write`).
//! - The persist lane (`lane*`, `ledger`) owns mutation persistence in every
//!   mode.
//! - [`CoreWarmSet`] owns native logical read state: typed definitions,
//!   complete selected keys, sparse current tips, confirmed absence, and full
//!   atom objects. Calls hold their selected paths; a separate recency list
//!   controls retention. An immutable typed core view and a held disk root
//!   supply each call's state when later writes change the current model.
//! - [`logical_set::LogicalResidentSet`] retains compatibility read state for
//!   the non-native LastStore adapter (`storage/laststore/logical_path/`).
//!
//! A separate byte-budgeted cache (`LASTDB_HASH_GROUP_WARM_BYTES`) covers the
//! compatibility adapter's non-logical collections. Its removal requires a
//! complete caller cutover, not a source cleanup.
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
pub(crate) mod core_keys;
mod core_set;
mod graph;
mod lane;
mod ledger;
mod logical_set;
mod metrics;
mod types;
mod worker;

pub use config::{
    parse_resident_bytes, parse_resident_max_deferred, parse_resident_mode,
    parse_resident_persist_interval, ResidentMode, ResidentPolicy, DEFAULT_RESIDENT_BYTES,
    DEFAULT_RESIDENT_MAX_DEFERRED, DEFAULT_RESIDENT_PERSIST_MS, RESIDENT_BYTES_ENV,
    RESIDENT_MAX_DEFERRED_ENV, RESIDENT_MODE_ENV, RESIDENT_PERSIST_MS_ENV,
};
pub(crate) use core_set::CoreMemorySource;
pub use core_set::{CoreMemoryStats, CoreMemoryValue};
pub(crate) use core_set::{
    CoreModelChange, CoreModelSnapshot, CoreModelStats, CoreModelUpdate, PreparedCoreMemory,
};
pub(crate) use core_set::{CorePath, CoreWarmLookup, CoreWarmSet, CoreWarmSlot};
pub(crate) use graph::OwnedSlotRead;
pub(crate) use graph::ResidentOverlayEntry;
pub use graph::{ResidentGraph, SchemaLoader};
pub use lane::{
    PersistBatchOutcome, PersistEnvelope, PersistLaneFull, PersistLaneKey, PersistLaneOccupancy,
    PersistLanePressure, PersistLaneSet, PersistLaneWriter, PersistReservation,
    PersistSlotRevision,
};
pub use logical_set::{
    init_resident_key_cap_from_env, parse_resident_key_cap, resident_key_cap, AtomId,
    DurabilityToken, FieldEntry, HashCompleteness, LogicalResidentSet, MoleculeId, ReadEpochs,
    ResidentKey, Tip, RESIDENT_KEY_CAP, RESIDENT_KEY_CAP_ENV,
};
pub use metrics::{ResidentMetrics, ResidentMetricsSnapshot};
pub use types::{
    DirtyKey, ResidentAtom, ResidentKeySetCompleteness, ResidentKeySetSnapshot, ResidentKind,
    ResidentMoleculeKey, ResidentSlotControl, ResidentSlotState, ResidentTip, ResolveOutcome,
    ResolveSource, RESIDENT_KIND_ATOM, RESIDENT_KIND_FILE_BLOB, RESIDENT_KIND_MOLECULE,
    RESIDENT_KIND_PROTEIN, RESIDENT_KIND_SCHEMA,
};
pub use worker::BackgroundPersistTask;
