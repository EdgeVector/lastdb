//! **QoS admission gate** — the fairness layer that keeps a bulk workload (large
//! blob reads/writes, e.g. a lastgit pack clone+push loop) from starving
//! interactive traffic (fbrain/fkanban point reads and small writes) on ONE
//! canonical node.
//!
//! # Why this exists
//!
//! Every owner-socket read and write ultimately dispatches its storage IO onto a
//! single shared `tokio::spawn_blocking` pool feeding one sled instance with a
//! single whole-DB write/flush log. With no admission control a burst of large
//! blob operations (a) occupies blocking-pool workers for the duration of their
//! big `apply_batch` + `flush`, and (b) parks sled's IO log in `make_stable`, so
//! a small interactive commit queues *behind* the big fsync — the "writer parked
//! in `apply_batch -> wait_for_readers`, readers queued behind it" pathology, and
//! the pool-starvation one where "a subsequent write's Sled persist can't get a
//! worker" (both documented in `fold_db` storage). The old full-node behaviour
//! made this worse by *hard-rejecting* on saturation with a "too many concurrent
//! reads" `503` while the minimal daemon had no gate at all.
//!
//! # What the gate does
//!
//! Two lanes share a global concurrency budget:
//! - **`Interactive`** — small/bounded reads and writes (fbrain get, fkanban
//!   add). These acquire only the global budget.
//! - **`Bulk`** — heavy operations (large write payloads, raw atom-content
//!   fetches, unbounded full scans). These acquire a *bulk* slot first, then the
//!   global budget.
//!
//! Because the bulk lane is capped strictly below the global budget
//! (`bulk_permits < total_permits`), at most `bulk_permits` global slots can be
//! held by bulk work at any instant, so **`total_permits - bulk_permits` global
//! slots are always reachable by interactive work** — the interactive lane can
//! never be starved below that reservation. Capping concurrent bulk operations
//! well below the blocking-pool size also guarantees an interactive op can always
//! get a blocking-pool worker for its sled persist, and keeps the flush queue an
//! interactive commit waits behind short.
//!
//! Saturation degrades **gracefully**: instead of an immediate hard reject, an
//! acquisition waits up to a per-lane deadline (bounded queueing). Only if the
//! deadline elapses does it shed with [`ReadBusy`] (→ `503`), so a transient
//! burst queues and drains rather than erroring.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::host_node::{ReadBusy, ReadPermit};

/// Which fairness lane an operation belongs to. Classified server-side from the
/// operation's shape/size — no client cooperation required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// Small, bounded, latency-sensitive work (fbrain get, fkanban add). Holds a
    /// reserved share of the global budget that bulk work cannot consume.
    Interactive,
    /// Heavy work (large write payloads, raw atom-content fetches, unbounded full
    /// scans — e.g. lastgit pack blob read/write). Capped strictly below the
    /// global budget so it cannot monopolise the node.
    Bulk,
}

/// Byte size at or above which a write payload (sum of a mutation batch's
/// serialized field values) is treated as [`Lane::Bulk`]. fbrain/fkanban rows are
/// well under this; lastgit pack blobs are well over it.
pub const BULK_WRITE_BYTES: usize = 64 * 1024;

/// Result-set cardinality at or above which a read is treated as [`Lane::Bulk`].
/// Point reads and small board enumerations (fbrain get, fkanban list) stay well
/// under this; a bulk clone enumerating thousands of stored objects crosses it.
pub const BULK_READ_ROWS: usize = 2_000;

impl Lane {
    /// Classify a write by the total serialized byte size of its mutation
    /// payload(s): a large blob write is [`Lane::Bulk`], a small row write is
    /// [`Lane::Interactive`].
    #[must_use]
    pub fn for_write_bytes(total_bytes: usize) -> Self {
        if total_bytes >= BULK_WRITE_BYTES {
            Self::Bulk
        } else {
            Self::Interactive
        }
    }

    /// Classify a read by the cardinality of the result set it will materialize
    /// (known cheaply from the pagination push-down's exact count): a large
    /// enumeration is [`Lane::Bulk`], a bounded page is [`Lane::Interactive`].
    #[must_use]
    pub fn for_read_rows(total_rows: usize) -> Self {
        if total_rows >= BULK_READ_ROWS {
            Self::Bulk
        } else {
            Self::Interactive
        }
    }

    /// Classify a read whose cardinality is *unknown* but whose fetch is bounded
    /// by a `Page` / `PageAfter` limit. Full-cap materializations
    /// ([`crate::pagination::INTERNAL_FETCH_CAP`]) walk up to 10k rows and must
    /// not sneak onto the interactive lane; smaller bounded pages stay light.
    #[must_use]
    pub fn for_page_limit(limit: usize) -> Self {
        // INTERNAL_FETCH_CAP is 10_000; keep the threshold here so qos stays free
        // of a pagination import cycle. Matches the injected full-cap Page path.
        const FULL_CAP_PAGE: usize = 10_000;
        if limit >= FULL_CAP_PAGE {
            Self::Bulk
        } else {
            Self::Interactive
        }
    }
}

/// Tunable capacities and deadlines for a [`QosGate`].
#[derive(Debug, Clone, Copy)]
pub struct QosConfig {
    /// Hard cap on globally concurrent DB operations (both lanes combined).
    pub total_permits: usize,
    /// Cap on concurrent [`Lane::Bulk`] operations. MUST be `< total_permits` so
    /// the interactive reservation (`total_permits - bulk_permits`) is positive.
    pub bulk_permits: usize,
    /// How long an [`Lane::Interactive`] acquisition may queue before shedding.
    pub interactive_deadline: Duration,
    /// How long a [`Lane::Bulk`] acquisition may queue before shedding.
    pub bulk_deadline: Duration,
    /// `Retry-After` hint (seconds) carried on a shed [`ReadBusy`].
    pub retry_after_secs: u64,
}

impl Default for QosConfig {
    fn default() -> Self {
        // Mini defaults are tuned for a colocated primary: brain/board share the
        // node with LastGit forge, pack IO, and agent fan-out. Keep a generous
        // global budget, but cap bulk at 1/8 so ~7/8 of slots stay reachable by
        // interactive work and concurrent bulk ops stay well below the
        // blocking-pool size. Override with LASTDB_QOS_TOTAL / LASTDB_QOS_BULK.
        Self {
            total_permits: 64,
            bulk_permits: 8,
            interactive_deadline: Duration::from_secs(30),
            bulk_deadline: Duration::from_secs(60),
            retry_after_secs: 1,
        }
    }
}

impl QosConfig {
    /// Build a config from the environment, falling back to [`Self::default`] for
    /// any unset/invalid knob:
    /// - `LASTDB_QOS_TOTAL` — global budget.
    /// - `LASTDB_QOS_BULK` — bulk-lane cap.
    /// - `LASTDB_QOS_DISABLE=1` — effectively disable gating (huge budgets, both
    ///   lanes unbounded) for A/B measurement or an emergency bypass.
    ///
    /// The result is always normalized via [`Self::normalized`] so an operator
    /// cannot configure a zero budget or a bulk cap that erases the interactive
    /// reservation.
    #[must_use]
    pub fn from_env() -> Self {
        if env_flag::var_truthy("LASTDB_QOS_DISABLE") {
            return Self {
                total_permits: Semaphore::MAX_PERMITS,
                bulk_permits: Semaphore::MAX_PERMITS,
                ..Self::default()
            };
        }
        let defaults = Self::default();
        let total =
            env_flag::var_parsed::<usize>("LASTDB_QOS_TOTAL").unwrap_or(defaults.total_permits);
        let bulk =
            env_flag::var_parsed::<usize>("LASTDB_QOS_BULK").unwrap_or(defaults.bulk_permits);
        Self {
            total_permits: total,
            bulk_permits: bulk,
            ..defaults
        }
        .normalized()
    }

    /// Clamp the config into a sane, deadlock-free shape: at least one global
    /// permit, and a bulk cap in `1..=total-1` so the interactive reservation is
    /// always positive (and, when disabled, both stay unbounded).
    #[must_use]
    pub fn normalized(self) -> Self {
        if self.total_permits >= Semaphore::MAX_PERMITS {
            return self; // disabled bypass — leave unbounded.
        }
        let total = self.total_permits.max(1);
        // Reserve at least one interactive slot: bulk in 1..=total-1 (or just the
        // single slot shared when total == 1).
        let bulk = if total == 1 {
            1
        } else {
            self.bulk_permits.clamp(1, total - 1)
        };
        Self {
            total_permits: total,
            bulk_permits: bulk,
            ..self
        }
    }
}

/// Live counters for the QoS gate — exposed on `/api/status` so operators can
/// see saturation without reading process internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QosSnapshot {
    pub total_permits: usize,
    pub bulk_permits: usize,
    /// Global slots currently held (both lanes).
    pub total_in_use: usize,
    /// Bulk-lane slots currently held.
    pub bulk_in_use: usize,
    /// Cumulative interactive acquisitions that timed out → 503.
    pub interactive_sheds: u64,
    /// Cumulative bulk acquisitions that timed out → 503.
    pub bulk_sheds: u64,
}

/// The admission gate. Cheap to clone-share via the two inner `Arc<Semaphore>`s;
/// a node holds exactly one and every owner-socket read/write acquires through
/// it.
#[derive(Clone)]
pub struct QosGate {
    total: Arc<Semaphore>,
    bulk: Arc<Semaphore>,
    config: QosConfig,
    interactive_sheds: Arc<AtomicU64>,
    bulk_sheds: Arc<AtomicU64>,
}

impl QosGate {
    /// Construct a gate from `config` (normalized first).
    #[must_use]
    pub fn new(config: QosConfig) -> Self {
        let config = config.normalized();
        Self {
            total: Arc::new(Semaphore::new(config.total_permits)),
            bulk: Arc::new(Semaphore::new(config.bulk_permits)),
            config,
            interactive_sheds: Arc::new(AtomicU64::new(0)),
            bulk_sheds: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The gate an operator's environment asks for ([`QosConfig::from_env`]).
    #[must_use]
    pub fn from_env() -> Self {
        Self::new(QosConfig::from_env())
    }

    /// The effective (normalized) configuration this gate runs with.
    #[must_use]
    pub fn config(&self) -> QosConfig {
        self.config
    }

    /// Point-in-time occupancy + cumulative shed counters for status surfaces.
    #[must_use]
    pub fn snapshot(&self) -> QosSnapshot {
        let total_available = self.total.available_permits();
        let bulk_available = self.bulk.available_permits();
        QosSnapshot {
            total_permits: self.config.total_permits,
            bulk_permits: self.config.bulk_permits,
            total_in_use: self.config.total_permits.saturating_sub(total_available),
            bulk_in_use: self.config.bulk_permits.saturating_sub(bulk_available),
            interactive_sheds: self.interactive_sheds.load(Ordering::Relaxed),
            bulk_sheds: self.bulk_sheds.load(Ordering::Relaxed),
        }
    }

    /// Acquire an admission slot for `lane`, queueing up to the lane's deadline.
    ///
    /// A [`Lane::Bulk`] acquisition takes a bulk slot first, then a global slot,
    /// so at most `bulk_permits` global slots are ever held by bulk work and the
    /// interactive reservation is preserved. Ordering is fixed (bulk → global for
    /// bulk; global only for interactive), so the two lanes cannot deadlock.
    ///
    /// # Errors
    /// [`ReadBusy`] if the lane's deadline elapses before a slot is free — the
    /// graceful shed the caller maps to a `503` with a retry hint.
    pub async fn acquire(&self, lane: Lane) -> Result<QosPermit, ReadBusy> {
        let busy = ReadBusy {
            retry_after_secs: self.config.retry_after_secs,
        };
        match lane {
            Lane::Interactive => {
                let deadline = Instant::now() + self.config.interactive_deadline;
                if let Some(global) = acquire_by(&self.total, deadline).await {
                    Ok(QosPermit {
                        _lane: None,
                        _global: global,
                    })
                } else {
                    self.interactive_sheds.fetch_add(1, Ordering::Relaxed);
                    Err(busy)
                }
            }
            Lane::Bulk => {
                let deadline = Instant::now() + self.config.bulk_deadline;
                let Some(lane_permit) = acquire_by(&self.bulk, deadline).await else {
                    self.bulk_sheds.fetch_add(1, Ordering::Relaxed);
                    return Err(busy);
                };
                if let Some(global) = acquire_by(&self.total, deadline).await {
                    Ok(QosPermit {
                        _lane: Some(lane_permit),
                        _global: global,
                    })
                } else {
                    // Dropping lane_permit releases the bulk slot.
                    drop(lane_permit);
                    self.bulk_sheds.fetch_add(1, Ordering::Relaxed);
                    Err(busy)
                }
            }
        }
    }
}

/// A held admission slot. Dropping it releases the global slot (and, for a bulk
/// op, the bulk slot). Held for the duration of the DB-touching operation so a
/// burst queues on the gate rather than thrashing the shared blocking pool + sled
/// flush log.
pub struct QosPermit {
    // Bulk ops hold their lane slot until drop; interactive ops carry `None`.
    _lane: Option<OwnedSemaphorePermit>,
    _global: OwnedSemaphorePermit,
}

// A `QosPermit` is `Send`, so the blanket impl in `host_node` already makes it a
// `ReadPermit`; this is the explicit boxing helper the hosts return.
impl QosPermit {
    /// Box this permit as the trait object the [`HostNode`](crate::HostNode)
    /// surface returns.
    #[must_use]
    pub fn boxed(self) -> Box<dyn ReadPermit> {
        Box::new(self)
    }
}

/// Wait for one permit from `sem` until `deadline`. `Some` on success, `None` on
/// timeout. The semaphore is never closed, so `acquire_owned` cannot error for
/// any other reason.
async fn acquire_by(sem: &Arc<Semaphore>, deadline: Instant) -> Option<OwnedSemaphorePermit> {
    tokio::time::timeout_at(deadline, Arc::clone(sem).acquire_owned())
        .await
        .ok()
        .and_then(Result::ok)
}
