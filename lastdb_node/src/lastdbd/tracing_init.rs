//! Tracing setup: fixed-field stderr and Sentry ERROR layer.

pub(crate) fn init_tracing() -> Option<observability::layers::error::SentryGuard> {
    use observability::layers::error::{build_error_layer, OBS_SENTRY_RELEASE_ENV};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    std::env::set_var(
        OBS_SENTRY_RELEASE_ENV,
        lastdb_node::version::build_version(),
    );

    let initial_directive = std::env::var("RUST_LOG")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .filter(|v| tracing_subscriber::EnvFilter::try_new(v).is_ok())
        .unwrap_or_else(|| "info".to_string());
    let env_filter = tracing_subscriber::EnvFilter::try_new(&initial_directive)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .event_format(super::private_diagnostics::PrivateFormat);
    let (error_layer, sentry_guard) = match build_error_layer() {
        Some((layer, guard)) => (Some(layer), Some(guard)),
        None => (None, None),
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(error_layer)
        .init();

    sentry_guard
}
