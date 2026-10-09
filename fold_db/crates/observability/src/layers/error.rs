//! ERROR layer — promote `tracing::error!` events into Sentry issues.
//!
//! Phase 4 / T4. The layer wraps [`sentry_tracing::layer`] with its own
//! per-layer filter ([`FilterFn`]) so Sentry only ever sees ERROR-level
//! events, regardless of how loose the global RELOAD filter is.
//! Each captured Sentry event is enriched with the W3C `trace_id` and
//! `span_id` lifted off the parent span's [`OtelData`] extension (set by
//! `tracing-opentelemetry`) so an alert page can be cross-referenced against
//! local logs filtered by the same trace id.
//!
//! ## When the layer is wired
//!
//! [`build_error_layer`] returns `None` whenever `OBS_SENTRY_DSN` is unset,
//! making the layer a strict opt-in. When set, the layer takes ownership of a
//! [`sentry::ClientInitGuard`] (re-exported as [`SentryGuard`]) that the
//! caller must hold for the lifetime of the binary — dropping it flushes any
//! buffered events.
//!
//! ## Layer composition
//!
//! Sentry is one of several sinks the registry feeds. Composing with
//! `Layer::with_filter` keeps the ERROR-only filter local to this layer and
//! independent of the rest of the pipeline. Concretely, the node binary will
//! end up with:
//!
//! ```text
//! Registry::default()
//!     .with(reload_layer)        // global RELOAD filter (info/debug/...)
//!     .with(otel_layer)          // attaches OtelData -> spans
//!     .with(fmt_layer)           // JSONL on disk
//!     .with(ring_layer)          // /api/logs
//!     .with(web_layer)           // SSE fan-out
//!     .with(error_layer)         // <-- this layer, ERROR-only Sentry sink
//! ```

#[cfg(feature = "sentry")]
use std::{env, time::Duration};

#[cfg(feature = "sentry")]
use sentry_tracing::EventMapping;
#[cfg(feature = "sentry")]
use tracing::{Level, Metadata, Subscriber};
#[cfg(all(feature = "sentry", feature = "otel"))]
use tracing_opentelemetry::OtelData;
#[cfg(feature = "sentry")]
use tracing_subscriber::filter::{filter_fn, FilterFn, Filtered};
#[cfg(feature = "sentry")]
use tracing_subscriber::layer::Layer;
#[cfg(feature = "sentry")]
use tracing_subscriber::registry::LookupSpan;

/// Environment variable that gates Sentry initialization. When unset (or
/// empty), [`build_error_layer`] returns `None` and the rest of the pipeline
/// runs unchanged.
pub const OBS_SENTRY_DSN_ENV: &str = "OBS_SENTRY_DSN";

/// Environment variable that names the Sentry `environment` (e.g.
/// `"production"` / `"development"`) the client tags every event with. When
/// unset (or empty) the SDK default applies and dev vs prod desktop events are
/// indistinguishable — which is the gap this var closes: the desktop node sets
/// it from its build profile (see `fold_db_node::telemetry_consent`), parallel
/// to the frontend's `environment: import.meta.env.PROD ? 'production' :
/// 'development'`. Read once at [`build_error_layer`] time.
pub const OBS_SENTRY_ENVIRONMENT_ENV: &str = "OBS_SENTRY_ENVIRONMENT";

/// Environment variable that names the Sentry `release` the client tags every
/// event with. When unset (or empty) `release` falls back to this crate's
/// `CARGO_PKG_VERSION` — which for the `observability` crate is a static `0.1.0`
/// that has nothing to do with the running app's version. That fallback is why
/// desktop events historically all reported `release: 0.1.0` and could not be
/// attributed to a real build (see fbrain
/// `papercut-telemetry-sentry-release-tag-stuck-0-1-0`).
///
/// The binary sets this from its baked build version — the same source as
/// `FOLDDB_BUILD_VERSION` / `fold_db_node::build_info::VERSION` — so events carry
/// the real crate/app version. Read once at [`build_error_layer`] time.
pub const OBS_SENTRY_RELEASE_ENV: &str = "OBS_SENTRY_RELEASE";

/// Environment variable carrying a stable, anonymous install identifier that is
/// attached as an `install_id` tag on every Sentry event. It lets distinct
/// machines be told apart (and separated from the maintainer's own boxes / agent
/// test runs) without any PII — it is a random UUID persisted in the profile
/// dir, not derived from anything user-identifying. When unset (or empty) no tag
/// is added. Read once at [`build_error_layer`] time and applied to the global
/// Sentry scope so it rides along with every captured event.
pub const OBS_SENTRY_INSTALL_ID_ENV: &str = "OBS_SENTRY_INSTALL_ID";

/// RAII guard returned by [`build_error_layer`].
///
/// Wraps the [`sentry::ClientInitGuard`] that the SDK hands back from
/// [`sentry::init`]. Holding the guard keeps the Sentry transport alive;
/// dropping it triggers a final flush. Stored inside [`crate::ObsGuard`] so
/// the binary's existing lifetime contract carries over.
#[must_use = "SentryGuard must be held for the lifetime of the binary or buffered events may be dropped"]
#[cfg(feature = "sentry")]
pub struct SentryGuard {
    _client: sentry::ClientInitGuard,
}

#[cfg(not(feature = "sentry"))]
pub struct SentryGuard;

#[cfg(feature = "sentry")]
impl std::fmt::Debug for SentryGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentryGuard").finish_non_exhaustive()
    }
}

#[cfg(not(feature = "sentry"))]
impl std::fmt::Debug for SentryGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentryGuard")
            .field("compiled", &false)
            .finish()
    }
}

/// Capture a runtime error on the active Sentry hub.
///
/// The tracing layer already promotes `tracing::error!` events into Sentry
/// events, but Lambda runtime failures are more useful as Sentry exception
/// events because the SDK preserves the error chain.
///
/// Use the main process hub rather than the current thread-local hub: Lambda
/// handlers run on Tokio worker threads, while [`build_error_layer`] installs
/// the Sentry client on the main thread during cold start. When no Sentry
/// client is configured this is a no-op and returns the SDK's nil event id.
#[cfg(feature = "sentry")]
pub fn capture_error(error: &(dyn std::error::Error + 'static)) {
    let hub = sentry::Hub::main();
    let event_id = hub.capture_error(error);
    if let Some(client) = hub.client() {
        client.flush(Some(Duration::from_secs(2)));
    }
    let _ = event_id;
}

#[cfg(not(feature = "sentry"))]
pub fn capture_error(_error: &(dyn std::error::Error + 'static)) {
    // Sentry intentionally absent from this build. Keep the public API as a
    // cheap no-op so callers do not need feature-specific branches.
}

/// Concrete return type of [`build_error_layer`]. Wrapping `SentryLayer`
/// inside a [`Filtered`] keeps the ERROR-only filter scoped to this layer
/// (the global RELOAD filter is left alone) and lets callers compose the
/// result with `.with(...)` without naming the layer's full type.
///
/// The wrapped filter is a [`FilterFn`] (not a plain `EnvFilter`) because
/// per-layer `EnvFilter("error")` would also hide INFO/DEBUG *spans* from
/// the layer — `Context::event_span` then returns `None` and we lose the
/// trace context. Letting all spans through but only enabling ERROR
/// *events* keeps `event_span` working while still gating Sentry to errors.
#[cfg(feature = "sentry")]
pub type ErrorLayer<S> = Filtered<sentry_tracing::SentryLayer<S>, FilterFn, S>;

#[cfg(not(feature = "sentry"))]
pub struct ErrorLayer<S>(std::marker::PhantomData<fn(S)>);

#[cfg(not(feature = "sentry"))]
impl<S> tracing_subscriber::layer::Layer<S> for ErrorLayer<S> where S: tracing::Subscriber {}

/// Build the per-layer filter used by [`build_error_layer`]: pass every span
/// through (so trace context lookup succeeds in `on_event`) but only allow
/// `Level::ERROR` events to reach Sentry.
#[cfg(feature = "sentry")]
fn error_only_event_filter() -> FilterFn {
    filter_fn(|meta: &Metadata<'_>| {
        if meta.is_event() {
            *meta.level() == Level::ERROR
        } else {
            true
        }
    })
}

/// Parse `OBS_SENTRY_DSN` as a Sentry DSN without panicking.
///
/// Returns `None` when the value is empty, a `lastsecrets://` locator (or any
/// other non-DSN), or otherwise unparseable. Callers treat `None` as "Sentry
/// disabled" so invalid host env (e.g. routines injecting a secret locator)
/// never aborts `lastdbd` before the owner socket is bound.
///
/// Secret locators must be resolved by a process wrapper (see
/// `scripts/lastdbd/install-secondary-launch-agent.sh`); this binary never
/// treats them as raw DSNs.
#[cfg(feature = "sentry")]
pub fn parse_obs_sentry_dsn(raw: &str) -> Option<sentry::types::Dsn> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    // Explicit early reject so the warn message names the failure mode agents
    // hit when routinesd injects OBS_SENTRY_DSN=lastsecrets://… into children.
    if raw.starts_with("lastsecrets://") || raw.starts_with("lastsecrets:") {
        eprintln!(
            "observability: {OBS_SENTRY_DSN_ENV} is a lastsecrets locator \
             (not a Sentry DSN); Sentry disabled. Resolve the secret in a \
             process wrapper before starting lastdbd, or unset the var."
        );
        return None;
    }
    match raw.parse::<sentry::types::Dsn>() {
        Ok(dsn) => Some(dsn),
        Err(err) => {
            // Do not include the raw value — it may be a near-miss secret or
            // partial credential. The parse error is enough to diagnose.
            eprintln!("observability: invalid {OBS_SENTRY_DSN_ENV} ({err}); Sentry disabled");
            None
        }
    }
}

/// Build the Sentry ERROR-layer when `OBS_SENTRY_DSN` is set.
///
/// Returns `None` when the env var is unset, empty, or not a valid Sentry DSN
/// so callers can treat "no usable DSN" as a clean no-op without panicking.
/// Invalid / unresolvable values (including `lastsecrets://…` locators) log a
/// one-line warning to stderr and continue with Sentry disabled — never abort
/// boot. When the DSN is valid, the function:
///
/// 1. Calls [`sentry::init`] with `release = $OBS_SENTRY_RELEASE` (falling
///    back to this crate's `CARGO_PKG_VERSION` only when unset — see
///    [`OBS_SENTRY_RELEASE_ENV`]) and, when [`OBS_SENTRY_ENVIRONMENT_ENV`] is
///    set to a non-empty value, `environment = $OBS_SENTRY_ENVIRONMENT` so dev
///    and prod desktop events are distinguishable in Sentry. When
///    [`OBS_SENTRY_INSTALL_ID_ENV`] is non-empty, its value is attached as an
///    `install_id` tag on the global scope so every event can be traced to a
///    stable, anonymous install. The returned [`sentry::ClientInitGuard`] is
///    wrapped in [`SentryGuard`] for lifetime management.
/// 2. Builds a [`sentry_tracing::SentryLayer`] with a custom `event_mapper`
///    that lifts `trace_id` / `span_id` from the parent span's [`OtelData`]
///    extension and attaches them to the outgoing event as Sentry tags.
/// 3. Wraps the layer in [`Layer::with_filter`] using
///    [`error_only_event_filter`] so only `tracing::error!` events ever
///    reach Sentry while leaving span tracking untouched (a per-layer
///    `EnvFilter("error")` would also hide INFO/DEBUG spans, which would
///    break `Context::event_span` and lose the trace context).
#[cfg(feature = "sentry")]
pub fn build_error_layer<S>() -> Option<(ErrorLayer<S>, SentryGuard)>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let dsn_raw = match env::var(OBS_SENTRY_DSN_ENV) {
        Ok(v) if !v.is_empty() => v,
        _ => return None,
    };
    // Parse before sentry::init — the SDK panics on InvalidProjectId /
    // empty DSN project rather than returning a disabled client.
    let dsn = parse_obs_sentry_dsn(&dsn_raw)?;

    // Tag every event with the deployment environment when the binary
    // configured one (the desktop node derives it from its build profile;
    // see `fold_db_node::telemetry_consent`). An empty / unset value leaves
    // the SDK default so existing callers that never set it are unaffected.
    let environment = env::var(OBS_SENTRY_ENVIRONMENT_ENV)
        .ok()
        .filter(|e| !e.is_empty())
        .map(std::borrow::Cow::Owned);

    // Prefer the release the binary baked in (its real build version); fall
    // back to this crate's CARGO_PKG_VERSION only when unset. The bare fallback
    // is the observability crate's own 0.1.0, which is why every desktop event
    // used to report `release: 0.1.0` — the binary now sets OBS_SENTRY_RELEASE
    // from FOLDDB_BUILD_VERSION so events carry the real app version.
    let release = env::var(OBS_SENTRY_RELEASE_ENV)
        .ok()
        .filter(|r| !r.is_empty())
        .map_or_else(
            || std::borrow::Cow::Borrowed(env!("CARGO_PKG_VERSION")),
            std::borrow::Cow::Owned,
        );

    let options = sentry::ClientOptions {
        release: Some(release),
        environment,
        ..Default::default()
    };
    let client = sentry::init((dsn, options));

    // Attach a stable, anonymous install-id tag to the global scope so every
    // captured event carries it — this is how distinct machines (and the
    // maintainer's own boxes / agent test runs) are told apart without any PII.
    // The binary persists a random UUID in the profile dir and exports it via
    // OBS_SENTRY_INSTALL_ID; an unset/empty value simply adds no tag.
    if let Some(install_id) = env::var(OBS_SENTRY_INSTALL_ID_ENV)
        .ok()
        .filter(|id| !id.is_empty())
    {
        sentry::configure_scope(|scope| {
            scope.set_tag("install_id", install_id);
        });
    }

    let layer = sentry_tracing::layer()
        .event_mapper(event_mapper_with_trace_context::<S>)
        .with_filter(error_only_event_filter());

    Some((layer, SentryGuard { _client: client }))
}

#[cfg(not(feature = "sentry"))]
pub fn build_error_layer<S>() -> Option<(ErrorLayer<S>, SentryGuard)> {
    None
}

/// Custom `event_mapper` for [`sentry_tracing::SentryLayer`].
///
/// Every ERROR event is mapped to a [`sentry::protocol::Event`] (via the
/// crate's own [`sentry_tracing::event_from_event`] helper, which preserves
/// the standard message / target / fields layout). We then walk up to the
/// parent span via the `Context`, look up the [`OtelData`] extension that
/// `tracing-opentelemetry` attaches in `on_new_span`, and copy the W3C
/// `trace_id` / `span_id` onto the Sentry event as tags. When the span has
/// no `OtelData` (no OTel layer wired, or no parent span at all) we emit the
/// event without trace context — the event is still useful, it just won't
/// deep-link.
#[allow(
    clippy::needless_pass_by_value,
    reason = "sentry_tracing::SentryLayer::event_mapper callback signature — Context<'_, S> is passed by value by the layer"
)]
#[cfg(feature = "sentry")]
fn event_mapper_with_trace_context<S>(
    event: &tracing::Event<'_>,
    ctx: tracing_subscriber::layer::Context<'_, S>,
) -> EventMapping
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let mut sentry_event = sentry_tracing::event_from_event(event, Some(&ctx));
    if is_disk_full_storage_event(&sentry_event) {
        let mut breadcrumb = sentry_tracing::breadcrumb_from_event(event, Some(&ctx));
        breadcrumb.data.insert(
            "lastdbd.event_class".to_string(),
            serde_json::Value::String("disk_full_storage".to_string()),
        );
        return EventMapping::Breadcrumb(breadcrumb);
    }

    #[cfg(feature = "otel")]
    if let Some(span_ref) = ctx.event_span(event) {
        let exts = span_ref.extensions();
        if let Some(otel_data) = exts.get::<OtelData>() {
            if let Some(trace_id) = otel_data.builder.trace_id {
                sentry_event
                    .tags
                    .insert("trace_id".to_string(), format!("{trace_id:032x}"));
            }
            if let Some(span_id) = otel_data.builder.span_id {
                sentry_event
                    .tags
                    .insert("span_id".to_string(), format!("{span_id:016x}"));
            }
        }
    }
    #[cfg(not(feature = "otel"))]
    let _ = (event, ctx);

    EventMapping::Event(sentry_event)
}

#[cfg(feature = "sentry")]
fn is_disk_full_storage_event(event: &sentry::protocol::Event<'_>) -> bool {
    event_text(event).is_some_and(|text| {
        let lower = text.to_ascii_lowercase();
        (lower.contains("storagefull")
            || lower.contains("storage full")
            || lower.contains("no space left on device")
            || lower.contains("enospc"))
            && (lower.contains("sled")
                || lower.contains("storage")
                || lower.contains("db")
                || lower.contains("database"))
    })
}

#[cfg(feature = "sentry")]
fn event_text(event: &sentry::protocol::Event<'_>) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(message) = event.message.as_deref() {
        parts.push(message.to_string());
    }
    if let Some(logentry) = event.logentry.as_ref() {
        parts.push(logentry.message.clone());
        parts.extend(logentry.params.iter().map(ToString::to_string));
    }
    parts.extend(event.extra.values().map(ToString::to_string));
    (!parts.is_empty()).then(|| parts.join(" "))
}
