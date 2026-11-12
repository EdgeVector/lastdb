//! Optional Sentry ERROR sink with strict privacy allowlisting.

#[cfg(feature = "sentry")]
use sentry::protocol::Event;
#[cfg(feature = "sentry")]
use sentry_tracing::EventMapping;
#[cfg(feature = "sentry")]
use std::{borrow::Cow, collections::BTreeMap, env, sync::Arc};
#[cfg(feature = "sentry")]
use tracing::{Level, Metadata, Subscriber};
#[cfg(feature = "sentry")]
use tracing_subscriber::{
    filter::{filter_fn, FilterFn, Filtered},
    layer::Layer,
    registry::LookupSpan,
};

#[cfg(feature = "sentry")]
const OBS_SENTRY_DSN_ENV: &str = "OBS_SENTRY_DSN";
#[cfg(feature = "sentry")]
const OBS_SENTRY_ENVIRONMENT_ENV: &str = "OBS_SENTRY_ENVIRONMENT";

/// Environment variable that names the Sentry release version.
pub const OBS_SENTRY_RELEASE_ENV: &str = "OBS_SENTRY_RELEASE";

/// RAII guard returned by [`build_error_layer`].
#[must_use = "SentryGuard must be held for the lifetime of the binary or buffered events may be dropped"]
pub struct SentryGuard {
    #[cfg(feature = "sentry")]
    _client: sentry::ClientInitGuard,
}

impl std::fmt::Debug for SentryGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentryGuard").finish_non_exhaustive()
    }
}

/// Concrete return type of [`build_error_layer`].
#[cfg(feature = "sentry")]
pub type ErrorLayer<S> = Filtered<sentry_tracing::SentryLayer<S>, FilterFn, S>;

/// Stand-in layer when built without the `sentry` feature.
#[cfg(not(feature = "sentry"))]
pub struct ErrorLayer<S>(std::marker::PhantomData<fn(S)>);

#[cfg(not(feature = "sentry"))]
impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for ErrorLayer<S> {}

#[cfg(feature = "sentry")]
fn parse_obs_sentry_dsn(raw: &str) -> Option<sentry::types::Dsn> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.starts_with("lastsecrets://") || raw.starts_with("lastsecrets:") {
        eprintln!(
            "observability: {OBS_SENTRY_DSN_ENV} is a lastsecrets locator \
             (not a Sentry DSN); Sentry disabled. Resolve the secret in a \
             process wrapper before starting lastdbd, or unset the var."
        );
        return None;
    }
    raw.parse::<sentry::types::Dsn>()
        .inspect_err(|err| {
            eprintln!("observability: invalid {OBS_SENTRY_DSN_ENV} ({err}); Sentry disabled");
        })
        .ok()
}

/// Build the Sentry ERROR-layer when `OBS_SENTRY_DSN` is set to a valid DSN.
#[cfg(feature = "sentry")]
pub fn build_error_layer<S>() -> Option<(ErrorLayer<S>, SentryGuard)>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let dsn = parse_obs_sentry_dsn(&env::var(OBS_SENTRY_DSN_ENV).ok()?)?;
    let non_empty_env = |k| env::var(k).ok().filter(|v| !v.is_empty());
    let release = non_empty_env(OBS_SENTRY_RELEASE_ENV)
        .map_or(Cow::Borrowed(env!("CARGO_PKG_VERSION")), Cow::Owned);
    let options = sentry::ClientOptions {
        release: Some(release),
        environment: non_empty_env(OBS_SENTRY_ENVIRONMENT_ENV).map(Cow::Owned),
        default_integrations: false,
        send_default_pii: false,
        attach_stacktrace: false,
        max_breadcrumbs: 0,
        auto_session_tracking: false,
        before_send: Some(Arc::new(|event| Some(sanitize_event(&event)))),
        ..sentry::ClientOptions::default()
    };
    let client = sentry::init((dsn, options));
    let filter: FilterFn =
        filter_fn(|meta: &Metadata<'_>| !meta.is_event() || *meta.level() == Level::ERROR);
    let layer = sentry_tracing::layer()
        .event_mapper(event_mapper::<S>)
        .with_filter(filter);
    Some((layer, SentryGuard { _client: client }))
}

/// Return `None` when built without the `sentry` feature.
#[cfg(not(feature = "sentry"))]
pub fn build_error_layer<S>() -> Option<(ErrorLayer<S>, SentryGuard)> {
    None
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "sentry_tracing::SentryLayer::event_mapper callback signature passes Context<'_, S> by value"
)]
#[cfg(feature = "sentry")]
fn event_mapper<S>(
    event: &tracing::Event<'_>,
    ctx: tracing_subscriber::layer::Context<'_, S>,
) -> EventMapping
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let sentry_event = sentry_tracing::event_from_event(event, Some(&ctx));
    if is_disk_full_storage_event(&sentry_event) {
        let mut breadcrumb = sentry_tracing::breadcrumb_from_event(event, Some(&ctx));
        breadcrumb.data.insert(
            "lastdbd.event_class".into(),
            serde_json::Value::String("disk_full_storage".into()),
        );
        return EventMapping::Breadcrumb(breadcrumb);
    }
    EventMapping::Event(sentry_event)
}

#[cfg(feature = "sentry")]
fn is_disk_full_storage_event(event: &Event<'_>) -> bool {
    let mut parts: Vec<String> = event.message.iter().cloned().collect();
    if let Some(log) = &event.logentry {
        parts.push(log.message.clone());
        parts.extend(log.params.iter().map(ToString::to_string));
    }
    parts.extend(event.extra.values().map(ToString::to_string));
    let lower = parts.join(" ").to_ascii_lowercase();
    [
        "storagefull",
        "storage full",
        "no space left on device",
        "enospc",
    ]
    .iter()
    .any(|n| lower.contains(n))
        && ["storage", "db", "database"]
            .iter()
            .any(|n| lower.contains(n))
}

#[cfg(feature = "sentry")]
fn numeric_version(release: &str) -> Option<&str> {
    let version = release
        .strip_prefix('v')
        .unwrap_or(release)
        .split('-')
        .next()?;
    let parts: Vec<&str> = version.split('.').collect();
    (parts.len() == 3
        && parts
            .iter()
            .all(|p| (1..=6).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_digit())))
    .then_some(version)
}

#[cfg(feature = "sentry")]
fn sanitize_event(source: &Event<'_>) -> Event<'static> {
    let category = match source.tags.get("crash_kind").map(String::as_str) {
        Some("panic") => "panic",
        Some("startup_failed") => "startup_failed",
        Some("unclean_exit") => "unclean_exit",
        _ => "application_error",
    };
    Event {
        level: source.level,
        platform: "native".into(),
        message: Some(format!("Application fault: {category}")),
        fingerprint: vec!["private_fault".into(), category.into()].into(),
        release: source
            .release
            .as_deref()
            .and_then(numeric_version)
            .map(|v| v.to_owned().into()),
        environment: source
            .environment
            .as_deref()
            .filter(|v| {
                matches!(
                    *v,
                    "production" | "development" | "prod" | "dev" | "staging"
                )
            })
            .map(|v| v.to_owned().into()),
        tags: BTreeMap::from([
            ("fault_category".into(), category.into()),
            ("os".into(), env::consts::OS.into()),
        ]),
        ..Event::default()
    }
}
