//! Shared telemetry substrate for the LastDB workspace.

use once_cell::sync::OnceCell;
use serde_json::{json, Map, Value};
use std::{collections::HashSet, fmt, io, time::SystemTime, time::UNIX_EPOCH};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_log::LogTracer;
use tracing_subscriber::fmt::{format::Writer, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::{layer::SubscriberExt, registry::LookupSpan, EnvFilter, Registry};

static INIT_ONCE: OnceCell<()> = OnceCell::new();
#[cfg(feature = "sentry")]
static LAMBDA_SERVICE_NAME: OnceCell<&'static str> = OnceCell::new();

pub mod crash {
    use std::io::Write;
    pub fn write_panic_marker() {
        let _ = writeln!(std::io::stderr().lock(), "LastDB panic");
    }
    pub fn install_private_panic_hook() {
        std::panic::set_hook(Box::new(|_| write_panic_marker()));
    }
}

pub mod propagation {
    pub fn inject_w3c<T>(builder: T) -> T {
        builder
    }
}

pub mod layers {
    pub mod error {
        pub const OBS_SENTRY_RELEASE_ENV: &str = "OBS_SENTRY_RELEASE";

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

        pub use crate::error_imp::{build_error_layer, ErrorLayer};
    }
}

#[cfg(not(feature = "sentry"))]
mod error_imp {
    use crate::layers::error::SentryGuard;
    pub struct ErrorLayer<S>(std::marker::PhantomData<fn(S)>);
    impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for ErrorLayer<S> {}
    pub fn build_error_layer<S>() -> Option<(ErrorLayer<S>, SentryGuard)> {
        None
    }
}

#[cfg(feature = "sentry")]
mod error_imp {
    use crate::layers::error::{SentryGuard, OBS_SENTRY_RELEASE_ENV};
    use crate::LAMBDA_SERVICE_NAME;
    use sentry::protocol::Event as SentryEvent;
    use sentry_tracing::EventMapping;
    use std::{borrow::Cow, collections::BTreeMap, env, fmt::Write as _, sync::Arc};
    use tracing::{Level, Metadata, Subscriber};
    use tracing_subscriber::filter::{filter_fn, FilterFn, Filtered};
    use tracing_subscriber::{layer::Layer, registry::LookupSpan};

    const DSN_ENV: &str = "OBS_SENTRY_DSN";
    pub type ErrorLayer<S> = Filtered<sentry_tracing::SentryLayer<S>, FilterFn, S>;

    fn parse_dsn(raw: &str) -> Option<sentry::types::Dsn> {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        if raw.starts_with("lastsecrets:") {
            eprintln!("observability: {DSN_ENV} is a lastsecrets locator (not a Sentry DSN); Sentry disabled.");
            return None;
        }
        raw.parse()
            .inspect_err(|e| eprintln!("observability: invalid {DSN_ENV} ({e}); Sentry disabled"))
            .ok()
    }

    pub fn build_error_layer<S: Subscriber + for<'a> LookupSpan<'a>>(
    ) -> Option<(ErrorLayer<S>, SentryGuard)> {
        let dsn = parse_dsn(&env::var(DSN_ENV).ok()?)?;
        let non_empty = |k| env::var(k).ok().filter(|v| !v.is_empty());
        let opts = sentry::ClientOptions {
            release: Some(
                non_empty(OBS_SENTRY_RELEASE_ENV)
                    .map_or(Cow::Borrowed(env!("CARGO_PKG_VERSION")), Cow::Owned),
            ),
            environment: non_empty("OBS_SENTRY_ENVIRONMENT").map(Cow::Owned),
            default_integrations: false,
            send_default_pii: false,
            attach_stacktrace: false,
            max_breadcrumbs: 0,
            auto_session_tracking: false,
            before_send: Some(Arc::new(|ev| Some(sanitize(&ev)))),
            ..sentry::ClientOptions::default()
        };
        let _client = sentry::init((dsn, opts));
        let filter: FilterFn =
            filter_fn(|m: &Metadata<'_>| !m.is_event() || *m.level() == Level::ERROR);
        let layer = sentry_tracing::layer()
            .event_mapper(map_event::<S>)
            .with_filter(filter);
        Some((layer, SentryGuard { _client }))
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "sentry_tracing::SentryLayer::event_mapper callback signature passes Context<'_, S> by value"
    )]
    fn map_event<S: Subscriber + for<'a> LookupSpan<'a>>(
        event: &tracing::Event<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) -> EventMapping {
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
        if let Some(l) = &ev.logentry {
            let _ = write!(s, " {}", l.message);
            for p in &l.params {
                let _ = write!(s, " {p}");
            }
        }
        for v in ev.extra.values() {
            let _ = write!(s, " {v}");
        }
        let s = s.to_ascii_lowercase();
        "storagefull|storage full|no space left on device|enospc"
            .split('|')
            .any(|n| s.contains(n))
            && ["storage", "db", "database"].iter().any(|n| s.contains(n))
    }

    fn numeric_version(rel: &str) -> Option<&str> {
        let v = rel.strip_prefix('v').unwrap_or(rel).split('-').next()?;
        let mut p = v.split('.');
        ([p.next(), p.next(), p.next()].into_iter().all(|x| {
            x.is_some_and(|s| (1..=6).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit()))
        }) && p.next().is_none())
        .then_some(v)
    }

    fn sanitize(src: &SentryEvent<'_>) -> SentryEvent<'static> {
        let cat = match src.tags.get("crash_kind").map(String::as_str) {
            Some("panic") => "panic",
            Some("startup_failed") => "startup_failed",
            Some("unclean_exit") => "unclean_exit",
            _ => "application_error",
        };
        let env_tag = src.environment.as_deref().filter(|v| {
            "production|development|prod|dev|staging"
                .split('|')
                .any(|a| a == *v)
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

    fn is_ident(s: &str, dash: bool) -> bool {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || (dash && b == b'-'))
    }

    fn fault_tags(cat: &str) -> BTreeMap<String, String> {
        let env_if = |key: &str, ok: fn(&str) -> bool| {
            let r = env::var(key).ok()?;
            let v = r.trim();
            ok(v).then(|| v.to_owned())
        };
        let mut tags = BTreeMap::from([
            ("fault_category".into(), cat.into()),
            ("os".into(), env::consts::OS.into()),
        ]);
        if let Some(&s) = LAMBDA_SERVICE_NAME.get().filter(|s| is_ident(s, false)) {
            tags.insert("service".into(), s.into());
        }
        if let Some(v) = env_if("AWS_LAMBDA_FUNCTION_VERSION", |v| {
            v == "$LATEST" || (!v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
        }) {
            tags.insert("function_version".into(), v);
        }
        if let Some(v) = env_if("AWS_LAMBDA_FUNCTION_NAME", |v| is_ident(v, true)) {
            tags.insert("function_name".into(), v);
        }
        tags
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObsError {
    #[error("observability already initialized")]
    AlreadyInitialized,
    #[error("failed to install tracing subscriber: {0}")]
    SubscriberInstall(String),
}

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

pub fn init_lambda(service_name: &'static str, _version: &str) -> Result<ObsGuard, ObsError> {
    assert!(
        !service_name.trim().is_empty(),
        "service.name must be non-empty"
    );
    INIT_ONCE
        .set(())
        .map_err(|()| ObsError::AlreadyInitialized)?;
    #[cfg(feature = "sentry")]
    let _ = LAMBDA_SERVICE_NAME.set(service_name);
    let _ = LogTracer::init();
    let (writer, _worker) = tracing_appender::non_blocking(io::stdout());
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (error_layer, _sentry) = layers::error::build_error_layer().unzip();
    let extra = std::env::var("OBS_REDACT_EXTRA").unwrap_or_default();
    let deny_list = "auth_token,auth.token,password,api_key,api.key,secret,email,phone,ssn"
        .split(',')
        .chain(extra.split(',').map(str::trim).filter(|s| !s.is_empty()))
        .map(str::to_owned)
        .collect();
    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(RedactingFormat(deny_list))
        .with_writer(writer);
    let sub = Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .with(error_layer);
    tracing::subscriber::set_global_default(sub)
        .map_err(|e| ObsError::SubscriberInstall(e.to_string()))?;
    Ok(ObsGuard { _sentry, _worker })
}

struct RedactingFormat(HashSet<String>);

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
        let mut vis = JsonFieldVisitor(&self.0, None, Map::new());
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
            "body": vis.1.unwrap_or_default(),
            "target": meta.target(),
        });
        let map = obj.as_object_mut().expect("json object");
        if let Some(span) = ctx.lookup_current() {
            map.insert("span".into(), Value::from(span.name()));
        }
        if !vis.2.is_empty() {
            map.insert("attributes".into(), Value::Object(vis.2));
        }
        writeln!(writer, "{obj}")
    }
}

struct JsonFieldVisitor<'a>(&'a HashSet<String>, Option<String>, Map<String, Value>);

impl JsonFieldVisitor<'_> {
    fn put(&mut self, f: &Field, v: Value) {
        let k = f.name();
        if k == "message" {
            self.1 = Some(v.as_str().map_or_else(|| v.to_string(), str::to_owned));
        } else {
            self.2.insert(
                k.into(),
                if self.0.contains(k) {
                    "<redacted>".into()
                } else {
                    v
                },
            );
        }
    }
}

impl Visit for JsonFieldVisitor<'_> {
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
