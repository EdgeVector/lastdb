//! Read-only status views over a [`PersistLaneSet`]: depth, age, occupancy
//! and pressure.

use std::time::{Duration, Instant};

use super::*;

impl<T: Send + 'static> PersistLaneSet<T> {
    pub fn metrics(&self) -> &PersistLaneCounters {
        &self.metrics
    }

    /// Sum of queued and in-flight envelopes across lanes.
    pub fn depth(&self) -> u64 {
        self.metrics.depth()
    }

    /// Age of the oldest queued or in-flight envelope.
    pub fn oldest_age(&self) -> Option<Duration> {
        let now = Instant::now();
        let lanes = self.lanes.lock().expect("persist lanes map");
        let mut oldest: Option<Instant> = None;
        for lane in lanes.values() {
            let fifo = lane.fifo.lock().expect("persist lane fifo");
            let lane_oldest = fifo_oldest_enqueued_at(&fifo);
            if let Some(front) = lane_oldest {
                oldest = Some(match oldest {
                    Some(t) if t <= front => t,
                    _ => front,
                });
            }
        }
        oldest.map(|t| now.saturating_duration_since(t))
    }

    pub fn oldest_age_ms(&self) -> u64 {
        self.oldest_age()
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }

    #[must_use]
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Occupancy of every live lane. Status-path cheap: one map lock plus
    /// one fifo lock per lane.
    #[must_use]
    pub fn occupancy(&self) -> Vec<PersistLaneOccupancy> {
        let lanes = self.lanes.lock().expect("persist lanes map");
        lanes
            .values()
            .map(|lane| {
                let fifo = lane.fifo.lock().expect("persist lane fifo");
                PersistLaneOccupancy {
                    storage_prefix: lane.key.storage_prefix.clone(),
                    schema_name: lane.key.schema_name.clone(),
                    queued_bytes: fifo.bytes,
                    reserved_bytes: fifo.reserved_bytes,
                    queued_entries: fifo.queue.len() as u64,
                    reserved_entries: fifo.reserved_entries as u64,
                    unhealthy: fifo.unhealthy,
                }
            })
            .collect()
    }

    #[must_use]
    pub fn heaviest_occupancy(&self) -> Option<PersistLaneOccupancy> {
        self.occupancy()
            .into_iter()
            .max_by_key(PersistLaneOccupancy::occupied_bytes)
            .filter(|row| row.occupied_bytes() > 0)
    }

    #[must_use]
    pub fn pressure(&self) -> PersistLanePressure {
        let heaviest = self.heaviest_occupancy();
        PersistLanePressure {
            fair_share_bytes: self.max_bytes,
            heaviest_schema: heaviest
                .as_ref()
                .map_or_else(String::new, |row| row.schema_name.clone()),
            heaviest_bytes: heaviest
                .as_ref()
                .map_or(0, PersistLaneOccupancy::occupied_bytes),
            queued_entries: heaviest.as_ref().map_or(0, |row| row.queued_entries),
            reserved_entries: heaviest.as_ref().map_or(0, |row| row.reserved_entries),
            oldest_age_ms: self.oldest_age_ms(),
            refuse_bytes: self.metrics.refuse_bytes(),
            refuse_entries: self.metrics.refuse_entries(),
            refuse_unhealthy: self.metrics.refuse_unhealthy(),
            unhealthy_lanes: self.metrics.unhealthy_lanes(),
            quarantined: self.metrics.quarantined(),
            breaker_trips: self.metrics.breaker_trips(),
        }
    }
}
