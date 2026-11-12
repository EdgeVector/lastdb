//! Optional Sentry ERROR sink with strict privacy allowlisting.

#[cfg(feature = "sentry")]
use std::{collections::BTreeMap, env};

#[cfg(feature = "sentry")]
use sentry::protocol::Event;
#[cfg(feature = "sentry")]
use sentry_tracing::EventMapping;
#[cfg(feature = "sentry")]
use tracing::{Level, Metadata, Subscriber};
#[cfg(feature = "sentry")]
use tracing_subscriber::filter::{filter_fn, FilterFn, Filtered};
#[cfg(feature = "sentry")]
use tracing_subscriber::layer::Layer;
#[cfg(feature = "sentry")]
use tracing_subscriber::registry::LookupSpan;

/// Environment variable that gates Sentry initialization.
pub const OBS_SENTRY_DSN_ENV: &str = "OBS_SENTRY_DSN";

/// Environment variable that names the Sentry deployment environment.
pub const OBS_SENTRY_ENVIRONMENT_ENV: &str = "OBS_SENTRY_ENVIRONMENT";

/// Environment variable that names the Sentry release version.
pub const OBS_SENTRY_RELEASE_ENV: &str = "OBS_SENTRY_RELEASE";

/// RAII guard returned by [`build_error_layer`].
#[must_use = "SentryGuard must be held for the lifetime of the binary or buffered events may be dropped"]
#[cfg(feature = "sentry")]
pub struct SentryGuard {
    _client: sentry::ClientInitGuard,
}

/// Empty guard used when built without the `sentry` feature.
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

/// Concrete return type of [`build_error_layer`].
#[cfg(feature = "sentry")]
pub type ErrorLayer<S> = Filtered<sentry_tracing::SentryLayer<S>, FilterFn, S>;

/// Stand-in layer when built without the `sentry` feature.
#[cfg(not(feature = "sentry"))]
pub struct ErrorLayer<S>(std::marker::PhantomData<fn(S)>);

#[cfg(not(feature = "sentry"))]
impl<S> tracing_subscriber::layer::Layer<S> for ErrorLayer<S> where S: tracing::Subscriber {}

/// Parse `OBS_SENTRY_DSN` as a Sentry DSN without panicking.
#[cfg(feature = "sentry")]
pub fn parse_obs_sentry_dsn(raw: &str) -> Option<sentry::types::Dsn> {
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
    match raw.parse::<sentry::types::Dsn>() {
        Ok(dsn) => Some(dsn),
        Err(err) => {
            eprintln!("observability: invalid {OBS_SENTRY_DSN_ENV} ({err}); Sentry disabled");
            None
        }
    }
}

/// Build the Sentry ERROR-layer when `OBS_SENTRY_DSN` is set to a valid DSN.
#[cfg(feature = "sentry")]
pub fn build_error_layer<S>() -> Option<(ErrorLayer<S>, SentryGuard)>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let dsn_raw = match env::var(OBS_SENTRY_DSN_ENV) {
        Ok(v) if !v.is_empty() => v,
        _ => return None,
    };
    let dsn = parse_obs_sentry_dsn(&dsn_raw)?;

    let environment = env::var(OBS_SENTRY_ENVIRONMENT_ENV)
        .ok()
        .filter(|e| !e.is_empty())
        .map(std::borrow::Cow::Owned);
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
        default_integrations: false,
        send_default_pii: false,
        attach_stacktrace: false,
        max_breadcrumbs: 0,
        auto_session_tracking: false,
        before_send: Some(std::sync::Arc::new(|event| Some(sanitize_event(&event)))),
        ..Default::default()
    };
    let client = sentry::init((dsn, options));
    let filter: FilterFn = filter_fn(is_error_or_span as fn(&Metadata<'_>) -> bool);
    let layer = sentry_tracing::layer()
        .event_mapper(event_mapper::<S>)
        .with_filter(filter);

    Some((layer, SentryGuard { _client: client }))
}

#[cfg(feature = "sentry")]
fn is_error_or_span(meta: &Metadata<'_>) -> bool {
    if meta.is_event() {
        *meta.level() == Level::ERROR
    } else {
        true
    }
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
            "lastdbd.event_class".to_string(),
            serde_json::Value::String("disk_full_storage".to_string()),
        );
        return EventMapping::Breadcrumb(breadcrumb);
    }
    EventMapping::Event(sentry_event)
}

#[cfg(feature = "sentry")]
fn is_disk_full_storage_event(event: &Event<'_>) -> bool {
    event_text(event).is_some_and(|text| {
        let lower = text.to_ascii_lowercase();
        (lower.contains("storagefull")
            || lower.contains("storage full")
            || lower.contains("no space left on device")
            || lower.contains("enospc"))
            && (lower.contains("storage") || lower.contains("db") || lower.contains("database"))
    })
}

#[cfg(feature = "sentry")]
fn event_text(event: &Event<'_>) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(message) = event.message.as_deref() {
        parts.push(message.to_string());
    }
    if let Some(logentry) = event.logentry.as_ref() {
        parts.push(logentry.message.clone());
        parts.extend(logentry.params.iter().map(ToString::to_string));
    }
    parts.extend(event.extra.values().map(ToString::to_string));
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
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
            .and_then(crate::crash::numeric_version)
            .map(str::to_string)
            .map(Into::into),
        environment: source.environment.as_deref().and_then(|value| match value {
            "production" | "development" | "prod" | "dev" | "staging" => {
                Some(value.to_string().into())
            }
            _ => None,
        }),
        tags: fault_tags(category),
        ..Default::default()
    }
}

#[cfg(feature = "sentry")]
fn fault_tags(category: &str) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::from([
        ("fault_category".to_string(), category.to_string()),
        ("os".to_string(), env::consts::OS.to_string()),
    ]);
    if let Some(service) = crate::init::lambda_service_name().and_then(service_tag) {
        tags.insert("service".to_string(), service);
    }
    if let Some(version) = function_version_tag() {
        tags.insert("function_version".to_string(), version);
    }
    if let Some(name) = function_name_tag() {
        tags.insert("function_name".to_string(), name);
    }
    tags
}

#[cfg(feature = "sentry")]
fn service_tag(value: &str) -> Option<String> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return None;
    }
    Some(value.to_string())
}

#[cfg(feature = "sentry")]
fn function_version_tag() -> Option<String> {
    let value = env::var("AWS_LAMBDA_FUNCTION_VERSION").ok()?;
    let value = value.trim();
    let allowed = value == "$LATEST"
        || (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()));
    allowed.then(|| value.to_string())
}

#[cfg(feature = "sentry")]
fn function_name_tag() -> Option<String> {
    let value = env::var("AWS_LAMBDA_FUNCTION_NAME").ok()?;
    let value = value.trim();
    let allowed = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    allowed.then(|| value.to_string())
}
