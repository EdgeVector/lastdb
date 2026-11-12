//! Shared telemetry substrate for the LastDB workspace.
#![warn(missing_docs)]

use std::{collections::HashSet, fmt, io, time::SystemTime, time::UNIX_EPOCH};

use once_cell::sync::OnceCell;
use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_log::LogTracer;
use tracing_subscriber::fmt::{format::Writer, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::{layer::SubscriberExt, registry::LookupSpan, EnvFilter, Registry};

static INIT_ONCE: OnceCell<()> = OnceCell::new();

static LAMBDA_SERVICE_NAME: OnceCell<&'static str> = OnceCell::new();

/// Service name recorded by [`init_lambda`].
pub(crate) fn lambda_service_name() -> Option<&'static str> {
    LAMBDA_SERVICE_NAME.get().copied()
}
const STATIC_DENY_LIST: &str =
    "auth_token,auth.token,password,api_key,api.key,secret,email,phone,ssn";

/// Panic marker hooks.
pub mod crash {
    use std::io::Write;
    /// Emit only a fixed marker to stderr.
    pub fn write_panic_marker() {
        let _ = writeln!(std::io::stderr().lock(), "LastDB panic");
    }
    /// Install fixed panic output so captured stderr cannot retain raw panic payloads.
    pub fn install_private_panic_hook() {
        std::panic::set_hook(Box::new(|_| write_panic_marker()));
    }
}

/// Outbound HTTP trace propagation hook.
pub mod propagation {
    /// Pass-through hook at outgoing HTTP call sites.
    pub fn inject_w3c<T>(builder: T) -> T {
        builder
    }
}

/// Subscriber layer namespaces.
pub mod layers {
    /// Optional Sentry ERROR sink with strict privacy allowlisting.
    pub mod error {
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

        #[cfg(not(feature = "sentry"))]
        mod imp {
            use super::SentryGuard;
            /// Stand-in layer when built without the `sentry` feature.
            pub struct ErrorLayer<S>(std::marker::PhantomData<fn(S)>);
            impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for ErrorLayer<S> {}
            /// Return `None` when built without the `sentry` feature.
            pub fn build_error_layer<S>() -> Option<(ErrorLayer<S>, SentryGuard)> {
                None
            }
        }

        #[cfg(feature = "sentry")]
        mod imp {
            use super::{SentryGuard, OBS_SENTRY_RELEASE_ENV};
            use sentry::protocol::Event as SentryEvent;
            use sentry_tracing::EventMapping;
            use std::{borrow::Cow, collections::BTreeMap, env, sync::Arc};
            use tracing::{Level, Metadata, Subscriber};
            use tracing_subscriber::{
                filter::{filter_fn, FilterFn, Filtered},
                layer::Layer,
                registry::LookupSpan,
            };

            const DSN_ENV: &str = "OBS_SENTRY_DSN";
            const ENV_ENV: &str = "OBS_SENTRY_ENVIRONMENT";

            /// Concrete return type of [`build_error_layer`].
            pub type ErrorLayer<S> = Filtered<sentry_tracing::SentryLayer<S>, FilterFn, S>;

            fn parse_dsn(raw: &str) -> Option<sentry::types::Dsn> {
                let raw = raw.trim();
                if raw.is_empty() {
                    return None;
                }
                if raw.starts_with("lastsecrets://") || raw.starts_with("lastsecrets:") {
                    eprintln!("observability: {DSN_ENV} is a lastsecrets locator (not a Sentry DSN); Sentry disabled.");
                    return None;
                }
                raw.parse()
                    .inspect_err(|e| {
                        eprintln!("observability: invalid {DSN_ENV} ({e}); Sentry disabled");
                    })
                    .ok()
            }

            /// Build the Sentry ERROR-layer when `OBS_SENTRY_DSN` is set to a valid DSN.
            pub fn build_error_layer<S>() -> Option<(ErrorLayer<S>, SentryGuard)>
            where
                S: Subscriber + for<'a> LookupSpan<'a>,
            {
                let dsn = parse_dsn(&env::var(DSN_ENV).ok()?)?;
                let non_empty = |k| env::var(k).ok().filter(|v| !v.is_empty());
                let release = non_empty(OBS_SENTRY_RELEASE_ENV)
                    .map_or(Cow::Borrowed(env!("CARGO_PKG_VERSION")), Cow::Owned);
                let opts = sentry::ClientOptions {
                    release: Some(release),
                    environment: non_empty(ENV_ENV).map(Cow::Owned),
                    default_integrations: false,
                    send_default_pii: false,
                    attach_stacktrace: false,
                    max_breadcrumbs: 0,
                    auto_session_tracking: false,
                    before_send: Some(Arc::new(|ev| Some(sanitize(&ev)))),
                    ..sentry::ClientOptions::default()
                };
                let client = sentry::init((dsn, opts));
                let filter: FilterFn =
                    filter_fn(|m: &Metadata<'_>| !m.is_event() || *m.level() == Level::ERROR);
                let layer = sentry_tracing::layer()
                    .event_mapper(map_event::<S>)
                    .with_filter(filter);
                Some((layer, SentryGuard { _client: client }))
            }

            #[allow(
                clippy::needless_pass_by_value,
                reason = "sentry_tracing::SentryLayer::event_mapper callback signature passes Context<'_, S> by value"
            )]
            fn map_event<S>(
                event: &tracing::Event<'_>,
                ctx: tracing_subscriber::layer::Context<'_, S>,
            ) -> EventMapping
            where
                S: Subscriber + for<'a> LookupSpan<'a>,
            {
                let ev = sentry_tracing::event_from_event(event, Some(&ctx));
                if is_disk_full(&ev) {
                    let mut b = sentry_tracing::breadcrumb_from_event(event, Some(&ctx));
                    b.data
                        .insert("lastdbd.event_class".into(), "disk_full_storage".into());
                    return EventMapping::Breadcrumb(b);
                }
                EventMapping::Event(ev)
            }

            fn is_disk_full(ev: &SentryEvent<'_>) -> bool {
                let mut s = ev.message.clone().unwrap_or_default();
                if let Some(log) = &ev.logentry {
                    s.push(' ');
                    s.push_str(&log.message);
                    log.params.iter().for_each(|p| s.push_str(&format!(" {p}")));
                }
                ev.extra.values().for_each(|v| s.push_str(&format!(" {v}")));
                let s = s.to_ascii_lowercase();
                [
                    "storagefull",
                    "storage full",
                    "no space left on device",
                    "enospc",
                ]
                .iter()
                .any(|n| s.contains(n))
                    && ["storage", "db", "database"].iter().any(|n| s.contains(n))
            }

            fn numeric_version(rel: &str) -> Option<&str> {
                let v = rel.strip_prefix('v').unwrap_or(rel).split('-').next()?;
                let mut p = v.split('.');
                let ok = [p.next(), p.next(), p.next()].into_iter().all(|x| {
                    x.is_some_and(|s| {
                        (1..=6).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit())
                    })
                }) && p.next().is_none();
                ok.then_some(v)
            }

            fn fault_tags(category: &str) -> BTreeMap<String, String> {
                let mut tags = BTreeMap::from([
                    ("fault_category".to_string(), category.to_string()),
                    ("os".to_string(), env::consts::OS.to_string()),
                ]);
                if let Some(service) = crate::lambda_service_name().and_then(service_tag) {
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

            fn function_version_tag() -> Option<String> {
                let value = env::var("AWS_LAMBDA_FUNCTION_VERSION").ok()?;
                let value = value.trim();
                let allowed = value == "$LATEST"
                    || (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()));
                allowed.then(|| value.to_string())
            }

            fn function_name_tag() -> Option<String> {
                let value = env::var("AWS_LAMBDA_FUNCTION_NAME").ok()?;
                let value = value.trim();
                let allowed = !value.is_empty()
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
                allowed.then(|| value.to_string())
            }

            fn sanitize(src: &SentryEvent<'_>) -> SentryEvent<'static> {
                let cat = match src.tags.get("crash_kind").map(String::as_str) {
                    Some("panic") => "panic",
                    Some("startup_failed") => "startup_failed",
                    Some("unclean_exit") => "unclean_exit",
                    _ => "application_error",
                };
                let env_tag = src.environment.as_deref().filter(|v| {
                    matches!(
                        *v,
                        "production" | "development" | "prod" | "dev" | "staging"
                    )
                });
                SentryEvent {
                    level: src.level,
                    platform: "native".into(),
                    message: Some(format!("Application fault: {cat}")),
                    fingerprint: vec!["private_fault".into(), cat.into()].into(),
                    release: src
                        .release
                        .as_deref()
                        .and_then(numeric_version)
                        .map(|v| v.to_owned().into()),
                    environment: env_tag.map(|v| v.to_owned().into()),
                    tags: fault_tags(cat),
                    ..SentryEvent::default()
                }
            }
        }

        pub use imp::{build_error_layer, ErrorLayer};
    }
}

/// Errors raised by [`init_lambda`].
#[derive(Debug, thiserror::Error)]
pub enum ObsError {
    /// `init_lambda` was called more than once in the same process.
    #[error("observability already initialized")]
    AlreadyInitialized,
    /// Could not install the global tracing subscriber.
    #[error("failed to install tracing subscriber: {0}")]
    SubscriberInstall(String),
}

/// RAII handle returned by [`init_lambda`].
#[must_use = "ObsGuard must be held for the lifetime of the binary or log lines may be dropped"]
pub struct ObsGuard {
    _sentry: Option<layers::error::SentryGuard>,
    _worker: WorkerGuard,
}

impl fmt::Debug for ObsGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObsGuard")
            .field("sentry_guard", &self._sentry.is_some())
            .finish_non_exhaustive()
    }
}

/// Initialize observability for an AWS Lambda handler.
pub fn init_lambda(service_name: &'static str, _version: &str) -> Result<ObsGuard, ObsError> {
    assert!(
        !service_name.trim().is_empty(),
        "service.name must be non-empty"
    );
    INIT_ONCE
        .set(())
        .map_err(|()| ObsError::AlreadyInitialized)?;
    let _ = LAMBDA_SERVICE_NAME.set(service_name);
    let _ = LogTracer::init();
    let (writer, _worker) = tracing_appender::non_blocking(io::stdout());
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (error_layer, _sentry) = layers::error::build_error_layer().unzip();
    let extra = std::env::var("OBS_REDACT_EXTRA").unwrap_or_default();
    let deny_list = STATIC_DENY_LIST
        .split(',')
        .chain(extra.split(',').map(str::trim).filter(|s| !s.is_empty()))
        .map(str::to_owned)
        .collect();
    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(RedactingFormat { deny_list })
        .with_writer(writer);
    let sub = Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .with(error_layer);
    tracing::subscriber::set_global_default(sub)
        .map_err(|e| ObsError::SubscriberInstall(e.to_string()))?;
    Ok(ObsGuard { _sentry, _worker })
}

struct RedactingFormat {
    deny_list: HashSet<String>,
}

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
        let mut vis = JsonFieldVisitor {
            deny_list: &self.deny_list,
            body: None,
            attributes: Map::new(),
        };
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
        let mut obj = Map::from_iter([
            ("time_unix_nano".into(), Value::String(nanos.to_string())),
            ("severity_text".into(), Value::String(level.to_string())),
            ("severity_number".into(), Value::from(sev)),
            ("body".into(), Value::String(vis.body.unwrap_or_default())),
            ("target".into(), Value::String(meta.target().to_string())),
        ]);
        if let Some(span) = ctx.lookup_current() {
            obj.insert("span".into(), Value::String(span.name().to_string()));
        }
        if !vis.attributes.is_empty() {
            obj.insert("attributes".into(), Value::Object(vis.attributes));
        }
        writeln!(
            writer,
            "{}",
            serde_json::to_string(&Value::Object(obj)).map_err(|_| fmt::Error)?
        )
    }
}

struct JsonFieldVisitor<'a> {
    deny_list: &'a HashSet<String>,
    body: Option<String>,
    attributes: Map<String, Value>,
}

impl JsonFieldVisitor<'_> {
    fn put(&mut self, name: &str, v: Value) {
        if name == "message" {
            self.body = Some(v.as_str().map_or_else(|| v.to_string(), str::to_owned));
        } else {
            let val = if self.deny_list.contains(name) {
                Value::String("<redacted>".into())
            } else {
                v
            };
            self.attributes.insert(name.to_owned(), val);
        }
    }
}

impl Visit for JsonFieldVisitor<'_> {
    fn record_str(&mut self, f: &Field, v: &str) {
        self.put(f.name(), Value::String(v.into()));
    }
    fn record_i64(&mut self, f: &Field, v: i64) {
        self.put(f.name(), Value::Number(v.into()));
    }
    fn record_u64(&mut self, f: &Field, v: u64) {
        self.put(f.name(), Value::Number(v.into()));
    }
    fn record_f64(&mut self, f: &Field, v: f64) {
        self.put(
            f.name(),
            serde_json::Number::from_f64(v).map_or(Value::Null, Value::Number),
        );
    }
    fn record_bool(&mut self, f: &Field, v: bool) {
        self.put(f.name(), Value::Bool(v));
    }
    fn record_debug(&mut self, f: &Field, v: &dyn fmt::Debug) {
        self.put(f.name(), Value::String(format!("{v:?}")));
    }
}
