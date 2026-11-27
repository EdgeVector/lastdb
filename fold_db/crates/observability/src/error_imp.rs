//! Sentry ERROR-layer: always-on fault surfaces, private event mapping.

use std::borrow::Cow;
use std::env;
use std::sync::Arc;

use sentry_tracing::EventMapping;
use tracing::{Level, Metadata, Subscriber};
use tracing_subscriber::filter::{filter_fn, FilterFn, Filtered};
use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

use crate::fault::{self, remember_environment};
use crate::layers::error::SentryGuard;
use crate::release::{RELEASE_SHA, USER_DEVICE_DSN};
use crate::{FaultSurface, ObsError};

const DSN_ENV: &str = "OBS_SENTRY_DSN";

pub type ErrorLayer<S> = Filtered<sentry_tracing::SentryLayer<S>, FilterFn, S>;

/// Initialize the Sentry client without a tracing layer (CLI / short tools).
pub fn init_sentry_client(surface: FaultSurface) -> Result<SentryGuard, ObsError> {
    let (dsn, opts) = prepare_client(surface)?;
    Ok(SentryGuard {
        _client: sentry::init((dsn, opts)),
    })
}

/// Build the ERROR-only Sentry tracing layer. Always installs a client.
pub fn build_error_layer<S: Subscriber + for<'a> LookupSpan<'a>>(
    surface: FaultSurface,
) -> Result<(ErrorLayer<S>, SentryGuard), ObsError> {
    let (dsn, opts) = prepare_client(surface)?;
    let _client = sentry::init((dsn, opts));
    let filter: FilterFn =
        filter_fn(|m: &Metadata<'_>| !m.is_event() || *m.level() == Level::ERROR);
    let layer = sentry_tracing::layer()
        .event_mapper(map_event::<S>)
        .with_filter(filter);
    Ok((layer, SentryGuard { _client }))
}

fn prepare_client(
    surface: FaultSurface,
) -> Result<(sentry::types::Dsn, sentry::ClientOptions), ObsError> {
    let (dsn, environment) = match surface {
        FaultSurface::User => {
            let dsn = parse_dsn(USER_DEVICE_DSN)?;
            (dsn, "user")
        }
        FaultSurface::Lambda { environment } => {
            let dsn = match env::var(DSN_ENV) {
                Ok(raw) if !raw.trim().is_empty() => parse_dsn(raw.trim())?,
                _ => return Err(ObsError::LambdaDsnRequired),
            };
            (dsn, environment)
        }
    };
    remember_environment(environment);
    let opts = sentry::ClientOptions {
        release: Some(Cow::Borrowed(RELEASE_SHA)),
        environment: Some(Cow::Borrowed(environment)),
        default_integrations: false,
        send_default_pii: false,
        attach_stacktrace: false,
        max_breadcrumbs: 0,
        server_name: None,
        before_send: Some(Arc::new(|ev| Some(fault::finalize(ev)))),
        before_breadcrumb: Some(Arc::new(|_| None)),
        ..sentry::ClientOptions::default()
    };
    Ok((dsn, opts))
}

fn parse_dsn(raw: &str) -> Result<sentry::types::Dsn, ObsError> {
    if raw.starts_with("lastsecrets:") {
        return Err(ObsError::InvalidDsn(
            "DSN is a lastsecrets locator, not a Sentry DSN".into(),
        ));
    }
    raw.parse::<sentry::types::Dsn>()
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
