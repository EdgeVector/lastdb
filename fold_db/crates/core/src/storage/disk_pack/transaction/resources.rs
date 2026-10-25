//! Critical-section ownership follows accepted work after caller cancellation.

use super::*;

pub(super) type Resource = Arc<dyn Send + Sync>;

#[derive(Clone)]
struct ScopedResource {
    identity: uuid::Uuid,
    value: Resource,
}

tokio::task_local! {
    static RESOURCES: Vec<ScopedResource>;
}

/// Each new batch inherits only resources for its exact native home.
pub(super) fn capture(runtime: &PackRuntime) -> Vec<Resource> {
    RESOURCES
        .try_with(|resources| {
            resources
                .iter()
                .filter(|resource| resource.identity == runtime.home_identity())
                .map(|resource| Arc::clone(&resource.value))
                .collect()
        })
        .unwrap_or_default()
}

/// The caller owns the resource until return; every accepted batch also owns
/// it until durable publication and delivery. A failed queue retains ownership.
pub(crate) async fn with_resource<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    resource: impl Send + Sync + 'static,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    let Some(runtime) = runtime else {
        let result = future.await;
        drop(resource);
        return result;
    };
    let value: Resource = Arc::new(resource);
    if let Some(pending) = PendingBatch::current(&runtime) {
        pending
            .resources
            .lock()
            .map_err(|_| StorageError::BackendError("native scoped resource lock poisoned".into()))?
            .push(Arc::clone(&value));
    }
    let mut resources = RESOURCES.try_with(Clone::clone).unwrap_or_default();
    resources.push(ScopedResource {
        identity: runtime.home_identity(),
        value,
    });
    RESOURCES.scope(resources, future).await
}

impl PendingBatch {
    /// Old read-prefix snapshots may retain this batch after completion. They
    /// must not prolong a completed critical section's queue-owned guard.
    pub(in crate::storage::disk_pack) fn release_resources(&self) {
        self.resources
            .lock()
            .expect("native completed scoped resources")
            .clear();
    }
}
