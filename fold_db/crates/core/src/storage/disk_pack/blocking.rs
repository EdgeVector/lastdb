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
    if can_block {
        tokio::task::block_in_place(work)
    } else {
        tokio::task::spawn_blocking(work).await?
    }
}
