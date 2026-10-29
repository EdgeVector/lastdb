use super::*;

impl<T: Send + 'static> PersistLaneSet<T> {
    pub fn new(
        max_entries: usize,
        max_bytes: u64,
        writer: Arc<dyn PersistLaneWriter<T>>,
        shared_metrics: Option<Arc<PersistLaneMetrics>>,
    ) -> Self {
        Self {
            admission: RwLock::new(()),
            lanes: Mutex::new(HashMap::new()),
            max_entries: max_entries.max(1),
            max_bytes: max_bytes.max(1),
            group_commit_max: 16,
            writer,
            metrics: Arc::new(PersistLaneCounters::default()),
            shared_metrics,
            age_tracker: Arc::new(PersistLaneAgeTracker::default()),
            stopped: AtomicBool::new(false),
            breaker_cooldown: breaker_cooldown_from_env(),
        }
    }

    /// Empty set that never spawns workers. Used by persist-surface clones
    /// that only run an already-dequeued envelope.
    pub fn disabled() -> Self {
        struct Noop;
        #[async_trait]
        impl<U: Send + 'static> PersistLaneWriter<U> for Noop {
            async fn write_batch(&self, _batch: Vec<PersistEnvelope<U>>) -> PersistBatchOutcome<U> {
                PersistBatchOutcome::Complete
            }
        }
        Self {
            admission: RwLock::new(()),
            lanes: Mutex::new(HashMap::new()),
            max_entries: 1,
            max_bytes: 1,
            group_commit_max: 1,
            writer: Arc::new(Noop),
            metrics: Arc::new(PersistLaneCounters::default()),
            shared_metrics: None,
            age_tracker: Arc::new(PersistLaneAgeTracker::default()),
            stopped: AtomicBool::new(true),
            breaker_cooldown: BREAKER_COOLDOWN_DEFAULT,
        }
    }

    fn entries_full(&self, fifo: &FifoState<T>) -> bool {
        fifo.queue.len().saturating_add(fifo.reserved_entries) >= self.max_entries
    }

    fn bytes_full(&self, fifo: &FifoState<T>, extra: u64) -> bool {
        let occupied = fifo.bytes.saturating_add(fifo.reserved_bytes);
        occupied.saturating_add(extra) > self.max_bytes && occupied > 0
    }

    /// Reserve one queue entry and `bytes` before apply-gate acquisition.
    ///
    /// A failed reserve is backpressure: the caller must not apply. Drop the
    /// reservation to return the slot; [`PersistReservation::fill`] enqueues.
    pub fn reserve(
        &self,
        key: PersistLaneKey,
        bytes: u64,
    ) -> Result<PersistReservation<T>, PersistLaneFull> {
        let _admission = self.admission.read().expect("persist lane admission");
        if self.stopped.load(Ordering::Acquire) {
            return Err(PersistLaneFull::Entries);
        }
        let lane = self.lane_for(key);
        {
            let mut fifo = lane.fifo.lock().expect("persist lane fifo");
            if fifo.unhealthy {
                self.metrics.record_refuse_unhealthy();
                return Err(PersistLaneFull::Unhealthy);
            }
            if self.entries_full(&fifo) {
                self.metrics.record_refuse_entries();
                return Err(PersistLaneFull::Entries);
            }
            if self.bytes_full(&fifo, bytes) {
                self.metrics.record_refuse_bytes();
                return Err(PersistLaneFull::Bytes);
            }
            fifo.reserved_entries = fifo.reserved_entries.saturating_add(1);
            fifo.reserved_bytes = fifo.reserved_bytes.saturating_add(bytes);
        }
        Ok(PersistReservation {
            lane,
            metrics: Arc::clone(&self.metrics),
            shared_metrics: self.shared_metrics.clone(),
            age_tracker: Arc::clone(&self.age_tracker),
            bytes,
            armed: true,
        })
    }

    /// Signal workers to drain and exit. Idempotent.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        let _admission = self.admission.write().expect("persist lane admission");
        let lanes: Vec<Arc<LaneRuntime<T>>> = self
            .lanes
            .lock()
            .expect("persist lanes map")
            .values()
            .cloned()
            .collect();
        for lane in lanes {
            lane.stop.store(true, Ordering::Relaxed);
            lane.notify.notify_one();
        }
    }

    /// Stop admission and wait for every lane worker to drain.
    ///
    /// Reserved, queued, and in-flight envelopes all delay completion. A
    /// timeout leaves each join handle in its lane so [`Drop`] can abort it.
    pub async fn wait_for_drain(&self, timeout: Duration) -> bool {
        self.stop();
        let lanes: Vec<Arc<LaneRuntime<T>>> = self
            .lanes
            .lock()
            .expect("persist lanes map")
            .values()
            .cloned()
            .collect();
        tokio::time::timeout(timeout, async move {
            for lane in lanes {
                wait_for_lane_drain(&lane).await;
            }
        })
        .await
        .is_ok()
    }

    /// Wait until no lane holds reserved, queued, or in-flight work.
    ///
    /// Admission stays open. Shutdown calls this first, while other
    /// subsystems still stop, so accepted writes become durable before a
    /// supervisor kill window can expire. A lane with a retained unhealthy
    /// head never drains by itself: when every busy lane is unhealthy, this
    /// returns `false` at once instead of at `timeout`.
    pub async fn wait_for_quiescent(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let (busy_healthy, busy_unhealthy) = {
                let lanes = self.lanes.lock().expect("persist lanes map");
                lanes
                    .values()
                    .fold((0_usize, 0_usize), |(healthy, unhealthy), lane| {
                        let fifo = lane.fifo.lock().expect("persist lane fifo");
                        let busy = !fifo.queue.is_empty()
                            || fifo.in_flight_oldest.is_some()
                            || fifo.reserved_entries > 0;
                        match (busy, fifo.unhealthy) {
                            (false, _) => (healthy, unhealthy),
                            (true, false) => (healthy + 1, unhealthy),
                            (true, true) => (healthy, unhealthy + 1),
                        }
                    })
            };
            if busy_healthy == 0 && busy_unhealthy == 0 {
                return true;
            }
            if busy_healthy == 0 {
                return false;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(QUIESCENT_POLL).await;
        }
    }

    fn lane_for(&self, key: PersistLaneKey) -> Arc<LaneRuntime<T>> {
        let mut lanes = self.lanes.lock().expect("persist lanes map");
        if let Some(existing) = lanes.get(&key) {
            return Arc::clone(existing);
        }
        let runtime = Arc::new(LaneRuntime {
            key: key.clone(),
            fifo: Mutex::new(FifoState {
                queue: VecDeque::new(),
                bytes: 0,
                in_flight_oldest: None,
                reserved_entries: 0,
                reserved_bytes: 0,
                next_commit_id: 0,
                consecutive_failures: 0,
                unhealthy: false,
                poisoned_head: None,
                quarantines_since_success: 0,
                breaker_open_since: None,
                breaker_trips: 0,
            }),
            notify: Notify::new(),
            // The admission read gate already accepted this caller. A
            // concurrent stop can set the set-level flag while it waits for
            // that gate, so this new lane must still start and then drain.
            stop: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            drain_notify: Notify::new(),
            join: Mutex::new(None),
        });
        let worker_lane = Arc::clone(&runtime);
        let writer = Arc::clone(&self.writer);
        let metrics = Arc::clone(&self.metrics);
        let shared_metrics = self.shared_metrics.clone();
        let age_tracker = Arc::clone(&self.age_tracker);
        let group_commit_max = self.group_commit_max;
        let breaker_cooldown = self.breaker_cooldown;
        // lint:spawn-bare-ok process-lifetime persist lane — not request-scoped.
        let handle = tokio::spawn(async move {
            run_lane_worker(
                worker_lane,
                writer,
                metrics,
                shared_metrics,
                age_tracker,
                group_commit_max,
                breaker_cooldown,
            )
            .await;
        });
        *runtime.join.lock().expect("persist lane join") = Some(handle);
        lanes.insert(key, Arc::clone(&runtime));
        runtime
    }
}

impl<T: Send + 'static> PersistReservation<T> {
    /// Convert this granted slot into a queued envelope and wake the worker.
    ///
    /// The reservation grants admission before resident apply. Thus, this
    /// operation cannot refuse after a later lane failure. The reserved byte
    /// charge replaces the envelope estimate, so accounting cannot shrink at
    /// the reserve-to-fill boundary.
    pub fn fill(mut self, mut envelope: PersistEnvelope<T>) -> u64 {
        let commit_id = {
            let mut fifo = self.lane.fifo.lock().expect("persist lane fifo");
            if self.armed {
                fifo.reserved_entries = fifo.reserved_entries.saturating_sub(1);
                fifo.reserved_bytes = fifo.reserved_bytes.saturating_sub(self.bytes);
                self.armed = false;
            }
            envelope.bytes = self.bytes;
            let commit_id = fifo.next_commit_id;
            fifo.next_commit_id = fifo.next_commit_id.saturating_add(1);
            envelope.commit_id = commit_id;
            fifo.bytes = fifo.bytes.saturating_add(envelope.bytes);
            fifo.queue.push_back(envelope);
            self.metrics.depth.fetch_add(1, Ordering::Relaxed);
            if let Some(shared) = &self.shared_metrics {
                shared.record_enqueue();
            }
            self.age_tracker.publish(
                &self.lane.key,
                fifo_oldest_enqueued_at(&fifo),
                self.shared_metrics.as_deref(),
            );
            commit_id
        };
        self.lane.notify.notify_one();
        commit_id
    }
}

impl<T: Send + 'static> Drop for PersistReservation<T> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut fifo = self.lane.fifo.lock().expect("persist lane fifo");
        fifo.reserved_entries = fifo.reserved_entries.saturating_sub(1);
        fifo.reserved_bytes = fifo.reserved_bytes.saturating_sub(self.bytes);
        self.armed = false;
        self.lane.notify.notify_one();
    }
}

impl<T: Send + 'static> Drop for PersistLaneSet<T> {
    fn drop(&mut self) {
        self.stop();
        let lanes: Vec<Arc<LaneRuntime<T>>> = self
            .lanes
            .lock()
            .expect("persist lanes map")
            .values()
            .cloned()
            .collect();
        for lane in lanes {
            if let Some(handle) = lane.join.lock().expect("persist lane join").take() {
                handle.abort();
            }
            self.age_tracker
                .publish(&lane.key, None, self.shared_metrics.as_deref());
        }
    }
}
