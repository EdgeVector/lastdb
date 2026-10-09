//! RELOAD layer — runtime `EnvFilter` updates via a [`ReloadHandle`].
//!
//! Wraps [`tracing_subscriber::reload`] so callers can swap filter directives
//! at runtime without re-installing the subscriber. This subsumes the legacy
//! `LoggingSystem::update_feature_level` capability and generalizes it to the
//! full `EnvFilter` directive syntax (e.g. `"my_crate::module=debug,info"`),
//! enabling per-target filtering rather than just a flat per-feature level.

use tracing::Subscriber;
use tracing_subscriber::reload;
use tracing_subscriber::EnvFilter;

/// Errors raised by [`ReloadHandle::update`].
#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    /// The supplied directive could not be parsed as an [`EnvFilter`].
    #[error("invalid filter directive: {0}")]
    Parse(String),
    /// The reload handle could not be applied (e.g. the subscriber was
    /// dropped or the inner lock is poisoned).
    #[error("failed to apply filter: {0}")]
    Apply(String),
}

/// Type-erased closure that parses a directive and reloads the wrapped layer.
type ApplyFn = dyn Fn(&str) -> Result<(), ReloadError> + Send + Sync;

/// Type-erased handle to swap the active [`EnvFilter`] at runtime.
///
/// Cloning the underlying handle is cheap (it stores an `Arc` internally), but
/// since this struct erases the subscriber type parameter we wrap it in a
/// boxed closure. Wrap in [`std::sync::Arc`] if multiple owners need it.
pub struct ReloadHandle {
    apply: Box<ApplyFn>,
}

impl ReloadHandle {
    /// Parse `directive` as an [`EnvFilter`] and install it as the active
    /// filter. Subsequent log events are filtered by the new directive.
    pub fn update(&self, directive: &str) -> Result<(), ReloadError> {
        (self.apply)(directive)
    }
}

impl std::fmt::Debug for ReloadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadHandle").finish_non_exhaustive()
    }
}

/// Build a reloadable [`EnvFilter`] layer.
///
/// Returns the layer (to be added to a `Registry`) and a [`ReloadHandle`]
/// that can be stored on the node / lambda / app context and exposed to
/// HTTP or IPC handlers for runtime filter updates.
pub fn build_reload_layer<S>(initial: EnvFilter) -> (reload::Layer<EnvFilter, S>, ReloadHandle)
where
    S: Subscriber,
{
    let (layer, handle) = reload::Layer::new(initial);
    let apply = Box::new(move |directive: &str| -> Result<(), ReloadError> {
        let filter =
            EnvFilter::try_new(directive).map_err(|e| ReloadError::Parse(e.to_string()))?;
        handle
            .reload(filter)
            .map_err(|e| ReloadError::Apply(e.to_string()))
    });
    (layer, ReloadHandle { apply })
}
