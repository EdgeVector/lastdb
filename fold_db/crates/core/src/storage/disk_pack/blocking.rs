//! The blocking-call bridge: native disk work runs off the async executor.

use crate::storage::error::StorageResult;

/// Run synchronous native disk work from an async caller. A multi-thread
/// runtime blocks in place; any other context uses a blocking task.
pub(in crate::storage) async fn run_blocking<T, F>(work: F) -> StorageResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> StorageResult<T> + Send + 'static,
{
    let can_block = tokio::runtime::Handle::try_current().is_ok_and(|handle| {
        matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        )
    });
    let disk_reads = crate::read_receipts::current();
    let queued = disk_reads.as_ref().map(|_| std::time::Instant::now());
    let observed_work = move || {
        crate::read_receipts::with_sync(disk_reads, || {
            if let Some(queued) = queued {
                laststore::record_queue_wait(queued.elapsed());
            }
            work()
        })
    };
    if can_block {
        tokio::task::block_in_place(observed_work)
    } else {
        tokio::task::spawn_blocking(observed_work).await?
    }
}
