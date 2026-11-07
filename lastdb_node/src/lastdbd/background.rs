//! Background ownership starts only after the daemon can serve both sockets.

use lastdb_node::host::Host;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::task::JoinHandle;

pub(crate) fn spawn(
    runtime: &tokio::runtime::Runtime,
    host: &Arc<Host>,
    shutdown: &Arc<AtomicBool>,
) -> (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>) {
    let sample_interval = lastdb_node::self_metrics::sample_interval_from_env();
    let _runtime_guard = runtime.enter();
    // fetch-blob downloads a record file that is not local (after a restore)
    // when this home has saved cloud credentials.
    lastdb_node::byte_cloud_backup::register_blob_source(host);
    (
        lastdb_node::jobs::self_metrics::spawn_sampler(
            Arc::clone(host),
            sample_interval,
            Arc::clone(shutdown),
        ),
        lastdb_node::byte_cloud_backup::spawn_automatic(Arc::clone(host), Arc::clone(shutdown)),
        lastdb_node::shared_sync::spawn_automatic(Arc::clone(host), Arc::clone(shutdown)),
    )
}
