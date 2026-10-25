//! Background ownership starts only after the daemon can serve both sockets.

use lastdb_node::host::Host;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::task::JoinHandle;

pub(crate) fn spawn(
    runtime: &tokio::runtime::Runtime,
    host: &Arc<Host>,
    shutdown: &Arc<AtomicBool>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let sample_interval = lastdb_node::self_metrics::sample_interval_from_env();
    let _runtime_guard = runtime.enter();
    (
        lastdb_node::self_metrics::spawn_sampler(
            Arc::clone(host),
            sample_interval,
            Arc::clone(shutdown),
        ),
        super::conflict_fold::spawn_home_conflict_fold(Arc::clone(host), Arc::clone(shutdown)),
    )
}
