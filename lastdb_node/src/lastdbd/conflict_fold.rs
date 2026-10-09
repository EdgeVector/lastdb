use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use lastdb_node::host::Host;

/// Fold `hcu:evt` into `hcu:mols` when pressure is clear.
///
/// The copy builder runs once at task start. It is a no-op unless
/// `LASTDB_BUILD_CONFLICT_STAMP_ON_COPY` is set, and that flag stays off on a
/// live daemon. The fold does not walk `conflict\0` or `mcc:`.
pub(crate) fn spawn_home_conflict_fold(
    host: Arc<Host>,
    shutdown: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(error) = host
            .db
            .db_ops()
            .build_home_conflict_stamp_on_copy(None)
            .await
        {
            tracing::warn!(
                target: "lastdbd::home_conflict_fold",
                error = %error,
                "home conflict stamp build on copy failed"
            );
        }
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        while !shutdown.load(Ordering::Acquire) {
            tokio::select! {
                _ = interval.tick() => {},
                _ = async {
                    while !shutdown.load(Ordering::Acquire) {
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    }
                } => break,
            }
            if shutdown.load(Ordering::Acquire) {
                break;
            }
            if !lastdb_node::footprint::conflict_fold_pressure_clear() {
                continue;
            }
            if let Err(error) = host.db.db_ops().fold_home_conflict_events(None).await {
                tracing::warn!(
                    target: "lastdbd::home_conflict_fold",
                    error = %error,
                    "home conflict event fold failed"
                );
            }
        }
    })
}
