//! Sentry ERROR-layer: DSN resolution, fail-closed PROD, private event mapping.

use std::borrow::Cow;
use std::env;
use std::sync::Arc;

use sentry_tracing::EventMapping;
use tracing::{Level, Metadata, Subscriber};
use tracing_subscriber::filter::{filter_fn, FilterFn, Filtered};
use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

use crate::fault::{self, is_prod_environment, resolved_environment};
use crate::layers::error::SentryGuard;
use crate::release::{EMBEDDED_DSN, RELEASE_SHA};
use crate::ObsError;

const DSN_ENV: &str = "OBS_SENTRY_DSN";

pub type ErrorLayer<S> = Filtered<sentry_tracing::SentryLayer<S>, FilterFn, S>;

/// Initialize the Sentry client without a tracing layer (CLI / short tools).
pub fn init_sentry_client() -> Result<Option<SentryGuard>, ObsError> {
    match prepare_client()? {
        Some((dsn, opts)) => Ok(Some(SentryGuard {
            _client: sentry::init((dsn, opts)),
        })),
        None => Ok(None),
    }
}

/// Build the ERROR-only Sentry tracing layer when a DSN is available.
pub fn build_error_layer<S: Subscriber + for<'a> LookupSpan<'a>>(
) -> Result<Option<(ErrorLayer<S>, SentryGuard)>, ObsError> {
    let Some((dsn, opts)) = prepare_client()? else {
        return Ok(None);
    };
    let _client = sentry::init((dsn, opts));
    let filter: FilterFn =
        filter_fn(|m: &Metadata<'_>| !m.is_event() || *m.level() == Level::ERROR);
    let layer = sentry_tracing::layer()
        .event_mapper(map_event::<S>)
        .with_filter(filter);
    Ok(Some((layer, SentryGuard { _client })))
}

fn prepare_client() -> Result<Option<(sentry::types::Dsn, sentry::ClientOptions)>, ObsError> {
    let env_name = resolved_environment();
    let dsn = match resolve_dsn() {
        Ok(Some(d)) => d,
        Ok(None) => {
            if env_name.as_deref().is_some_and(is_prod_environment) {
                return Err(ObsError::ProdDsnRequired);
            }
            return Ok(None);
        }
        Err(e) => {
            if env_name.as_deref().is_some_and(is_prod_environment) {
                return Err(e);
            }
            eprintln!("observability: {e}; Sentry disabled");
            return Ok(None);
        }
    };
    // Always set environment so sentry::apply_defaults cannot invent "production".
    let environment = Some(match env_name {
        Some(e) => Cow::Owned(e),
        None if cfg!(debug_assertions) => Cow::Borrowed("dev"),
        None => Cow::Borrowed("user"),
    });
    let opts = sentry::ClientOptions {
        release: Some(Cow::Borrowed(RELEASE_SHA)),
        environment,
        default_integrations: false,
        send_default_pii: false,
        attach_stacktrace: false,
        max_breadcrumbs: 0,
        server_name: None,
        before_send: Some(Arc::new(|ev| Some(fault::finalize(ev)))),
        before_breadcrumb: Some(Arc::new(|_| None)),
        ..sentry::ClientOptions::default()
    };
    Ok(Some((dsn, opts)))
}

fn resolve_dsn() -> Result<Option<sentry::types::Dsn>, ObsError> {
    if let Some(raw) = env::var(DSN_ENV)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
    {
        return parse_dsn(&raw);
    }
    if let Some(raw) = EMBEDDED_DSN.map(str::trim).filter(|v| !v.is_empty()) {
        return parse_dsn(raw);
    }
    Ok(None)
}

fn parse_dsn(raw: &str) -> Result<Option<sentry::types::Dsn>, ObsError> {
    if raw.starts_with("lastsecrets:") {
        return Err(ObsError::InvalidDsn(
            "OBS_SENTRY_DSN is a lastsecrets locator, not a Sentry DSN".into(),
        ));
    }
    raw.parse::<sentry::types::Dsn>()
        .map(Some)
        .map_err(|e| ObsError::InvalidDsn(e.to_string()))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "sentry_tracing::SentryLayer::event_mapper callback signature passes Context<'_, S> by value"
)]
fn map_event<S: Subscriber + for<'a> LookupSpan<'a>>(
    event: &tracing::Event<'_>,
    _ctx: tracing_subscriber::layer::Context<'_, S>,
) -> EventMapping {
    EventMapping::Event(fault::event_from_tracing(event))
}
