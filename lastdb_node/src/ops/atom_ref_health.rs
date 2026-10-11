//! Background refresh of the atom reverse-edge health gauges.
//!
//! The durable readiness markers can populate LastStore's warm set, so they
//! are read here on a background task, never on `/api/status`. Each sampler
//! tick can start one refresh, at most one at a time per host.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::host::Host;

#[derive(Debug, Default)]
pub struct AtomRefHealthRuntime {
    in_flight: AtomicBool,
}

impl AtomRefHealthRuntime {
    fn begin(&self) -> bool {
        self.in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn end(&self) {
        self.in_flight.store(false, Ordering::Release);
    }

    pub fn in_flight(&self) -> bool {
        self.in_flight.load(Ordering::Acquire)
    }
}

/// Start one health refresh, at most once per host at a time.
pub fn maybe_spawn_atom_ref_health_refresh(host: &std::sync::Arc<Host>) {
    if !host.atom_ref_health.begin() {
        return;
    }
    let host = std::sync::Arc::clone(host);
    tokio::spawn(async move {
        crate::maintenance_write_gate::run(&host, async {
            crate::self_metrics::refresh_atom_ref_edge_health(&host).await;
        })
        .await;
        host.atom_ref_health.end();
    });
}
