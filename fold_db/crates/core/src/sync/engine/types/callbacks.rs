use std::sync::Arc;

/// Async callback that reloads an in-memory cache from persistent storage.
/// Returns the number of newly added items, or an error string.
/// Used for both schema and embedding reloaders — same signature.
pub type ReloadCallback = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize, String>> + Send>>
        + Send
        + Sync,
>;

/// Callback that reloads schemas from the persistent store into the in-memory cache.
pub type SchemaReloadCallback = ReloadCallback;

/// Callback that reloads embeddings from the persistent store into the in-memory index.
pub type EmbeddingReloadCallback = ReloadCallback;

/// Apply a captured [`crate::sync::log::MutationEnvelope`] batch on a peer.
/// Capture must stay suppressed inside the callback.
pub type MutationIntentApplier = std::sync::Arc<
    dyn Fn(
            Vec<crate::sync::log::MutationEnvelope>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<(), crate::sync::MutationIntentReplayError>>
                    + Send,
            >,
        > + Send
        + Sync,
>;

/// Resolve ref-only mutation envelopes from the serving atom plane before
/// replay or before sealing an off-box payload.
pub type MutationIntentMaterializer = std::sync::Arc<
    dyn Fn(
            Vec<crate::sync::log::MutationEnvelope>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Vec<crate::sync::log::MutationEnvelope>, String>,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

/// Make all local writes acknowledged before a photograph cut visible to the
/// store that [`crate::sync::snapshot::Snapshot`] enumerates.
pub type PhotographCutBarrier = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;
