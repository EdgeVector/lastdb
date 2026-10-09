//! Local short-TTL mutation doorbell (not cloud-synced, not product truth).
//!
//! Apps sleep on `GET /api/local-watch` instead of polling keyed tables when
//! idle. Events are thin hints; clients still read real HashRange tables after
//! wake. Ring drops by age (~10 min) and max count; gaps force resync.

use fold_db::clock::unix_millis;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;

/// Default retention: short doorbell tape, not a second database.
pub const DEFAULT_TTL: Duration = Duration::from_secs(10 * 60);
/// Cap so a write storm cannot grow the ring unboundedly.
pub const DEFAULT_MAX_EVENTS: usize = 50_000;
/// Max long-poll wait clients may request.
pub const MAX_POLL_TIMEOUT: Duration = Duration::from_secs(60);

/// Thin mutation doorbell row.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LocalWatchEvent {
    pub seq: u64,
    pub ts_ms: u64,
    pub schema: String,
    pub mutation_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
}

#[derive(Debug)]
struct Inner {
    next_seq: u64,
    events: VecDeque<LocalWatchEvent>,
    /// Wall-clock insert time for TTL (not serialized).
    inserted_at: VecDeque<Instant>,
    ttl: Duration,
    max_events: usize,
}

/// Process-local mutation outbox: append on successful write, poll/wait for seq.
#[derive(Debug)]
pub struct LocalOutbox {
    inner: Mutex<Inner>,
    cvar: Condvar,
}

impl Default for LocalOutbox {
    fn default() -> Self {
        Self::new(DEFAULT_TTL, DEFAULT_MAX_EVENTS)
    }
}

impl LocalOutbox {
    #[must_use]
    pub fn new(ttl: Duration, max_events: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                next_seq: 1,
                events: VecDeque::new(),
                inserted_at: VecDeque::new(),
                ttl,
                max_events: max_events.max(1),
            }),
            cvar: Condvar::new(),
        }
    }

    #[must_use]
    pub fn shared(ttl: Duration, max_events: usize) -> Arc<Self> {
        Arc::new(Self::new(ttl, max_events))
    }

    fn prune_locked(inner: &mut Inner, now: Instant) {
        while let Some(front_at) = inner.inserted_at.front() {
            if now.duration_since(*front_at) <= inner.ttl {
                break;
            }
            inner.inserted_at.pop_front();
            inner.events.pop_front();
        }
        while inner.events.len() > inner.max_events {
            inner.inserted_at.pop_front();
            inner.events.pop_front();
        }
    }

    /// Append one thin event after a successful product mutation.
    pub fn append(
        &self,
        schema: impl Into<String>,
        mutation_type: impl Into<String>,
        hash: Option<String>,
        range: Option<String>,
    ) -> u64 {
        let mut guard = self.inner.lock().expect("local outbox mutex");
        let now = Instant::now();
        Self::prune_locked(&mut guard, now);
        let seq = guard.next_seq;
        guard.next_seq = guard.next_seq.saturating_add(1);
        guard.events.push_back(LocalWatchEvent {
            seq,
            ts_ms: unix_millis(),
            schema: schema.into(),
            mutation_type: mutation_type.into(),
            hash,
            range,
        });
        guard.inserted_at.push_back(now);
        Self::prune_locked(&mut guard, now);
        self.cvar.notify_all();
        seq
    }

    /// Current tip (next seq that will be assigned). `0` means never written.
    pub fn tip_seq(&self) -> u64 {
        let mut guard = self.inner.lock().expect("local outbox mutex");
        Self::prune_locked(&mut guard, Instant::now());
        guard.next_seq.saturating_sub(1)
    }

    /// Events with `seq > after_seq`. If none and `timeout` is zero, returns empty.
    /// If `after_seq` is behind the ring (gap), sets `gap=true`.
    pub fn poll_after(&self, after_seq: u64, timeout: Duration) -> LocalWatchPoll {
        self.poll_after_schemas(after_seq, timeout, None)
    }

    /// Events with `seq > after_seq` and, when provided, a schema in `schemas`.
    /// `tip_seq` always reports the true node tip so filtered clients can advance
    /// their cursor across unrelated writes.
    pub fn poll_after_schemas(
        &self,
        after_seq: u64,
        timeout: Duration,
        schemas: Option<&HashSet<String>>,
    ) -> LocalWatchPoll {
        self.poll_after_schemas_and_hash(after_seq, timeout, schemas, None)
    }

    /// Events with `seq > after_seq`, filtered by schema and one HashRange
    /// hash when provided. The hash filter lets a client watch one partition
    /// without waking for writes to other partitions in the same schema.
    pub fn poll_after_schemas_and_hash(
        &self,
        after_seq: u64,
        timeout: Duration,
        schemas: Option<&HashSet<String>>,
        hash: Option<&str>,
    ) -> LocalWatchPoll {
        self.poll_after_schemas_hash_range(after_seq, timeout, schemas, hash, None, None)
    }

    /// Events with `seq > after_seq`, filtered by schema, one HashRange hash,
    /// and an optional inclusive-start/exclusive-end range.
    pub fn poll_after_schemas_hash_range(
        &self,
        after_seq: u64,
        timeout: Duration,
        schemas: Option<&HashSet<String>>,
        hash: Option<&str>,
        range_start: Option<&str>,
        range_end: Option<&str>,
    ) -> LocalWatchPoll {
        let deadline = Instant::now() + timeout;
        let mut guard = self.inner.lock().expect("local outbox mutex");
        loop {
            let now = Instant::now();
            Self::prune_locked(&mut guard, now);

            let oldest = guard.events.front().map(|e| e.seq);
            let tip = guard.next_seq.saturating_sub(1);
            // after_seq=0 means "give me retained history" — never a gap.
            // Gap only when the client cursor is strictly behind the oldest retained seq.
            let gap = after_seq > 0
                && match oldest {
                    Some(o) => o > after_seq + 1,
                    None => tip > after_seq,
                };

            let events: Vec<LocalWatchEvent> = guard
                .events
                .iter()
                .filter(|e| e.seq > after_seq)
                .filter(|e| match schemas {
                    Some(schemas) => schemas.contains(&e.schema),
                    None => true,
                })
                .filter(|e| hash.is_none_or(|wanted| e.hash.as_deref() == Some(wanted)))
                .filter(|e| {
                    range_start.is_none_or(|start| e.range.as_deref().is_some_and(|r| r >= start))
                })
                .filter(|e| range_end.is_none_or(|end| e.range.as_deref().is_some_and(|r| r < end)))
                .cloned()
                .collect();

            if !events.is_empty() || timeout.is_zero() || Instant::now() >= deadline {
                return LocalWatchPoll {
                    after_seq,
                    tip_seq: tip,
                    gap,
                    events,
                };
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return LocalWatchPoll {
                    after_seq,
                    tip_seq: tip,
                    gap,
                    events: vec![],
                };
            }
            let (g, wait_res) = self
                .cvar
                .wait_timeout(guard, remaining)
                .expect("local outbox condvar");
            guard = g;
            if wait_res.timed_out() {
                let now = Instant::now();
                Self::prune_locked(&mut guard, now);
                let oldest = guard.events.front().map(|e| e.seq);
                let tip = guard.next_seq.saturating_sub(1);
                let gap = after_seq > 0
                    && match oldest {
                        Some(o) => o > after_seq + 1,
                        None => tip > after_seq,
                    };
                let events: Vec<LocalWatchEvent> = guard
                    .events
                    .iter()
                    .filter(|e| e.seq > after_seq)
                    .filter(|e| match schemas {
                        Some(schemas) => schemas.contains(&e.schema),
                        None => true,
                    })
                    .filter(|e| hash.is_none_or(|wanted| e.hash.as_deref() == Some(wanted)))
                    .filter(|e| {
                        range_start
                            .is_none_or(|start| e.range.as_deref().is_some_and(|r| r >= start))
                    })
                    .filter(|e| {
                        range_end.is_none_or(|end| e.range.as_deref().is_some_and(|r| r < end))
                    })
                    .cloned()
                    .collect();
                return LocalWatchPoll {
                    after_seq,
                    tip_seq: tip,
                    gap,
                    events,
                };
            }
        }
    }
}

/// Response body for `GET /api/local-watch`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LocalWatchPoll {
    pub after_seq: u64,
    pub tip_seq: u64,
    /// Client's cursor fell behind the retained ring — re-read product tables.
    pub gap: bool,
    pub events: Vec<LocalWatchEvent>,
}
