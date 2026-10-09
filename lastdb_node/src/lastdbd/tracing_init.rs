//! Tracing subscriber setup: stderr fmt, reloadable filter, Sentry ERROR layer.

pub(crate) fn init_tracing() -> Option<observability::layers::error::SentryGuard> {
    use observability::layers::error::build_error_layer;
    use observability::layers::reload::build_reload_layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Registry;

    // The binary's baked version is authoritative for the Sentry release, and
    // it must be installed BEFORE `build_error_layer` binds the client. Any
    // disagreeing operator value is reported after the subscriber exists — a
    // `warn!` emitted here would go nowhere.
    let ignored_release = lastdb_node::crash_attribution::install_baked_sentry_release();

    // The startup directive is resolved exactly as before — RUST_LOG when set,
    // otherwise `info`. It is captured as a string so the runtime control
    // surface can report the filter the process actually booted with;
    // `EnvFilter` itself only round-trips through `Display`, which normalizes
    // an empty directive set to the empty string, so an operator reading
    // `GET /api/system/log-filter` on a default boot sees `info` rather than
    // nothing.
    let initial_directive = std::env::var("RUST_LOG")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .filter(|v| tracing_subscriber::EnvFilter::try_new(v).is_ok())
        .unwrap_or_else(|| "info".to_string());
    let env_filter = tracing_subscriber::EnvFilter::try_new(&initial_directive)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // RELOAD wraps the filter so `POST /api/system/log-filter` can swap it in
    // place. Composition is otherwise byte-identical to the previous plain
    // `.with(env_filter)`: the reload layer is the same global filter, added
    // innermost, exactly as `observability::init_node` composes it.
    let (reload_layer, reload_handle) = build_reload_layer::<Registry>(env_filter);
    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    let (error_layer, sentry_guard) = match build_error_layer() {
        Some((layer, guard)) => (Some(layer), Some(guard)),
        None => (None, None),
    };

    tracing_subscriber::registry()
        .with(reload_layer)
        .with(fmt_layer)
        .with(error_layer)
        .init();

    // Only after the subscriber exists, so a rejected directive later has
    // somewhere to be logged.
    lastdb_node::ops::log_filter::install(reload_handle, initial_directive);

    if let Some(ignored) = ignored_release {
        tracing::warn!(
            ignored_release = %ignored,
            release = %lastdb_node::crash_attribution::build_version(),
            env = %observability::layers::error::OBS_SENTRY_RELEASE_ENV,
            "ignoring a configured Sentry release that disagrees with this binary's \
             baked build version; the baked version is authoritative and the env var \
             can be dropped from the launchd plist"
        );
    }

    sentry_guard
}
