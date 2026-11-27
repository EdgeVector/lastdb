//! Shared telemetry substrate for the LastDB workspace.

use once_cell::sync::OnceCell;
use serde_json::{json, Map, Value};
use std::{fmt, io, time::SystemTime, time::UNIX_EPOCH};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_log::LogTracer;
use tracing_subscriber::fmt::{format::Writer, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::{layer::SubscriberExt, registry::LookupSpan, EnvFilter, Registry};

static INIT_ONCE: OnceCell<()> = OnceCell::new();

pub mod crash;
pub mod release;

#[cfg(feature = "sentry")]
mod fault;

#[cfg(feature = "sentry")]
mod error_imp;

/// Where fault events are reported from. Environment is derived here, never from a toggle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultSurface {
    /// Installed lastdbd / CLI / People: environment is always `user`.
    User,
    /// Cloud Lambda: environment is the deploy stage (`dev` or `prod`).
    Lambda { environment: &'static str },
}

pub mod layers {
    pub mod error {
        #[must_use = "SentryGuard must be held for the lifetime of the binary or buffered events may be dropped"]
        pub struct SentryGuard {
            #[cfg(feature = "sentry")]
            pub(crate) _client: sentry::ClientInitGuard,
        }

        impl std::fmt::Debug for SentryGuard {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("SentryGuard").finish_non_exhaustive()
            }
        }

        #[cfg(feature = "sentry")]
        pub use crate::error_imp::{build_error_layer, init_sentry_client, ErrorLayer};

        #[cfg(not(feature = "sentry"))]
        pub type ErrorLayer<S> = FilteredNoop<S>;

        #[cfg(not(feature = "sentry"))]
        pub struct FilteredNoop<S>(std::marker::PhantomData<fn(S)>);

        #[cfg(not(feature = "sentry"))]
        impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for FilteredNoop<S> {}

        #[cfg(not(feature = "sentry"))]
        pub fn build_error_layer<S>(
            _surface: crate::FaultSurface,
        ) -> Result<(ErrorLayer<S>, SentryGuard), crate::ObsError> {
            Err(crate::ObsError::SentryRequired)
        }

        #[cfg(not(feature = "sentry"))]
        pub fn init_sentry_client(
            _surface: crate::FaultSurface,
        ) -> Result<SentryGuard, crate::ObsError> {
            Err(crate::ObsError::SentryRequired)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObsError {
    #[error("observability already initialized")]
    AlreadyInitialized,
    #[error("failed to install tracing subscriber: {0}")]
    SubscriberInstall(String),
    #[error("OBS_SENTRY_DSN is required for Lambda fault reporting")]
    LambdaDsnRequired,
    #[error("invalid Sentry DSN: {0}")]
    InvalidDsn(String),
    #[error("Lambda ENVIRONMENT must be dev or prod")]
    LambdaEnvironmentRequired,
    #[error("Sentry fault reporting is required but the sentry feature is not enabled")]
    SentryRequired,
}

#[must_use = "ObsGuard must be held for the lifetime of the binary or log lines may be dropped"]
pub struct ObsGuard {
    _sentry: layers::error::SentryGuard,
    _worker: WorkerGuard,
}

impl fmt::Debug for ObsGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObsGuard").finish_non_exhaustive()
    }
}

/// Record the service name used on Sentry `service` tags (no-op without `sentry`).
pub fn set_service_name(service_name: &'static str) {
    assert!(
        !service_name.trim().is_empty(),
        "service.name must be non-empty"
    );
    #[cfg(feature = "sentry")]
    fault::remember_service(service_name);
}

/// Derive the Lambda deploy environment from `ENVIRONMENT` (`dev` or `prod`).
pub fn lambda_environment_from_env() -> Result<&'static str, ObsError> {
    match std::env::var("ENVIRONMENT") {
        Ok(v) => match v.trim() {
            "prod" | "production" => Ok("prod"),
            "dev" | "development" => Ok("dev"),
            _ => Err(ObsError::LambdaEnvironmentRequired),
        },
        Err(_) => Err(ObsError::LambdaEnvironmentRequired),
    }
}

/// Initialize JSON logging and Sentry for a Lambda service.
pub fn init_lambda(service_name: &'static str, _version: &str) -> Result<ObsGuard, ObsError> {
    assert!(
        !service_name.trim().is_empty(),
        "service.name must be non-empty"
    );
    INIT_ONCE
        .set(())
        .map_err(|()| ObsError::AlreadyInitialized)?;
    crash::install_private_panic_hook();
    #[cfg(feature = "sentry")]
    fault::remember_service(service_name);
    let environment = lambda_environment_from_env()?;
    let _ = LogTracer::init();
    let (writer, _worker) = tracing_appender::non_blocking(io::stdout());
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (error_layer, _sentry) =
        layers::error::build_error_layer(FaultSurface::Lambda { environment })?;
    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(RedactingFormat)
        .with_writer(writer);
    let sub = Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .with(error_layer);
    tracing::subscriber::set_global_default(sub)
        .map_err(|e| ObsError::SubscriberInstall(e.to_string()))?;
    Ok(ObsGuard { _sentry, _worker })
}

struct RedactingFormat;

impl<S, N> FormatEvent<S, N> for RedactingFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let level = *meta.level();
        let mut vis = JsonFieldVisitor(None, Map::new());
        event.record(&mut vis);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let sev: u32 = match level {
            Level::TRACE => 1,
            Level::DEBUG => 5,
            Level::INFO => 9,
            Level::WARN => 13,
            Level::ERROR => 17,
        };
        let mut obj = json!({
            "time_unix_nano": nanos.to_string(),
            "severity_text": level.as_str(),
            "severity_number": sev,
            "body": vis.0.unwrap_or_default(),
            "target": meta.target(),
        });
        let map = obj.as_object_mut().expect("json object");
        if let Some(span) = ctx.lookup_current() {
            map.insert("span".into(), Value::from(span.name()));
        }
        if !vis.1.is_empty() {
            map.insert("attributes".into(), Value::Object(vis.1));
        }
        writeln!(writer, "{obj}")
    }
}

struct JsonFieldVisitor(Option<String>, Map<String, Value>);

impl JsonFieldVisitor {
    fn put(&mut self, f: &Field, v: Value) {
        let k = f.name();
        if k == "message" {
            self.0 = Some(v.as_str().map_or_else(|| v.to_string(), str::to_owned));
        } else {
            let redacted = matches!(
                k,
                "auth_token"
                    | "auth.token"
                    | "password"
                    | "api_key"
                    | "api.key"
                    | "secret"
                    | "email"
                    | "phone"
                    | "ssn"
            );
            self.1
                .insert(k.into(), if redacted { "<redacted>".into() } else { v });
        }
    }
}

impl Visit for JsonFieldVisitor {
    fn record_str(&mut self, f: &Field, v: &str) {
        self.put(f, Value::from(v));
    }
    fn record_i64(&mut self, f: &Field, v: i64) {
        self.put(f, Value::from(v));
    }
    fn record_u64(&mut self, f: &Field, v: u64) {
        self.put(f, Value::from(v));
    }
    fn record_f64(&mut self, f: &Field, v: f64) {
        self.put(
            f,
            serde_json::Number::from_f64(v).map_or(Value::Null, Value::Number),
        );
    }
    fn record_bool(&mut self, f: &Field, v: bool) {
        self.put(f, Value::Bool(v));
    }
    fn record_debug(&mut self, f: &Field, v: &dyn fmt::Debug) {
        self.put(f, Value::from(format!("{v:?}")));
    }
}
