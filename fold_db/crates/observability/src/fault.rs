//! Privacy-preserving Sentry events: fault code + callsite, code frames only.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::sync::OnceLock;

use sentry::protocol::{Event, Exception, Frame, Mechanism, Stacktrace};
use sentry::Level;
use tracing::field::{Field, Visit};
use tracing::{Event as TracingEvent, Metadata};

use crate::release::RELEASE_SHA;

static SERVICE_NAME: OnceLock<&'static str> = OnceLock::new();

const CODE_PREFIXES: &[&str] = &[
    "lastdb",
    "fold_db",
    "observability",
    "schema_service",
    "storage_service",
    "auth_service",
    "exemem",
    "mutation_log",
    "relay_protocol",
    "cloud_backup",
    "laststore",
    "database_policy",
    "app_identity",
];

/// Remember the Lambda / binary service name for the `service` tag.
pub fn remember_service(name: &'static str) {
    let _ = SERVICE_NAME.set(name);
}

pub fn service_name() -> Option<&'static str> {
    SERVICE_NAME.get().copied()
}

/// Build a private event from a `tracing` ERROR event.
pub fn event_from_tracing(event: &TracingEvent<'_>) -> Event<'static> {
    let meta = event.metadata();
    let mut vis = FaultVisitor::default();
    event.record(&mut vis);
    let fault_code = vis.fault_code.unwrap_or_else(|| derive_fault_code(meta));
    let location = callsite_location(meta);
    private_event(
        Level::Error,
        &fault_code,
        &location,
        code_stacktrace(),
        None,
    )
}

/// Report a panic: location always; message only for a `&'static str` payload.
#[allow(deprecated)] // PanicHookInfo arrives on newer rustc; PanicInfo stays portable.
pub fn capture_panic(info: &std::panic::PanicInfo<'_>) {
    let location = info.location().map_or_else(
        || "unknown".to_owned(),
        |l| format!("{}:{}", file_name(l.file()), l.line()),
    );
    let static_msg = info.payload().downcast_ref::<&'static str>().copied();
    let event = private_event(
        Level::Fatal,
        "panic",
        &location,
        code_stacktrace(),
        static_msg,
    );
    let hub = sentry::Hub::main();
    let _ = hub.capture_event(event);
    if let Some(client) = hub.client() {
        let _ = client.flush(Some(std::time::Duration::from_secs(2)));
    }
}

/// Final allowlist pass for every outbound event (tracing and panic).
pub fn finalize(mut src: Event<'static>) -> Event<'static> {
    let fault_code = src
        .tags
        .get("fault_code")
        .cloned()
        .filter(|v| is_fault_ident(v))
        .unwrap_or_else(|| "application_fault".into());
    let location = src
        .tags
        .get("fault_location")
        .cloned()
        .filter(|v| is_location(v))
        .unwrap_or_else(|| "unknown".into());
    let static_msg = src
        .message
        .as_deref()
        .filter(|m| m.starts_with("panic: ") && is_static_panic_message(m));
    let stack = src
        .exception
        .values
        .first()
        .and_then(|e| e.stacktrace.clone())
        .or_else(|| {
            src.threads
                .values
                .first()
                .and_then(|t| t.stacktrace.clone())
        })
        .map(|mut st| filter_code_frames(&mut st))
        .or_else(code_stacktrace);
    let mut out = private_event(src.level, &fault_code, &location, stack, None);
    if let Some(msg) = static_msg {
        out.message = Some(msg.to_owned());
    }
    out.release = Some(Cow::Borrowed(RELEASE_SHA));
    out.environment = src
        .environment
        .take()
        .filter(|e| is_allowed_env(e))
        .or_else(|| resolved_environment().map(Cow::Owned))
        .or(Some(Cow::Borrowed(if cfg!(debug_assertions) {
            "dev"
        } else {
            "user"
        })));
    out
}

fn private_event(
    level: Level,
    fault_code: &str,
    location: &str,
    stack: Option<Stacktrace>,
    static_panic_msg: Option<&str>,
) -> Event<'static> {
    let message = match static_panic_msg {
        Some(m) => format!("panic: {m}"),
        None => format!("{fault_code} @ {location}"),
    };
    let mut tags = base_tags();
    tags.insert("fault_code".into(), fault_code.to_owned());
    tags.insert("fault_location".into(), location.to_owned());
    Event {
        level,
        platform: "native".into(),
        message: Some(message),
        fingerprint: vec![fault_code.to_owned().into(), location.to_owned().into()].into(),
        release: Some(Cow::Borrowed(RELEASE_SHA)),
        environment: resolved_environment().map(Cow::Owned),
        tags,
        server_name: None,
        user: None,
        exception: vec![Exception {
            ty: fault_code.to_owned(),
            value: Some(location.to_owned()),
            mechanism: Some(Mechanism {
                ty: "fault_code".into(),
                handled: Some(level != Level::Fatal),
                ..Mechanism::default()
            }),
            stacktrace: stack,
            ..Exception::default()
        }]
        .into(),
        ..Event::default()
    }
}

fn base_tags() -> BTreeMap<String, String> {
    let mut tags = BTreeMap::from([("os".into(), env::consts::OS.into())]);
    if let Some(s) = service_name().filter(|s| is_ident(s, false)) {
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

pub fn resolved_environment() -> Option<String> {
    non_empty("OBS_SENTRY_ENVIRONMENT").filter(|v| is_allowed_env(v))
}

pub fn is_prod_environment(env: &str) -> bool {
    matches!(env, "prod" | "production")
}

pub fn is_allowed_env(v: &str) -> bool {
    matches!(
        v,
        "prod" | "production" | "dev" | "development" | "staging" | "user"
    )
}

fn derive_fault_code(meta: &Metadata<'_>) -> String {
    let raw = meta.target().replace("::", ".");
    if let Some(code) = sanitize_fault_ident(&raw) {
        return code;
    }
    "application_fault".into()
}

fn callsite_location(meta: &Metadata<'_>) -> String {
    let file = meta.file().map_or("unknown", file_name);
    let line = meta.line().unwrap_or(0);
    let func = meta.module_path().unwrap_or("unknown");
    format!("{func}::{file}:{line}")
}

fn code_stacktrace() -> Option<Stacktrace> {
    let mut st = sentry::integrations::backtrace::current_stacktrace()?;
    Some(filter_code_frames(&mut st))
}

fn filter_code_frames(st: &mut Stacktrace) -> Stacktrace {
    let mut frames: Vec<Frame> = st
        .frames
        .drain(..)
        .filter(frame_is_code)
        .map(scrub_frame)
        .collect();
    if frames.is_empty() {
        // Keep a single unknown frame so Sentry still shows a stack section.
        frames.push(Frame {
            function: Some("unknown".into()),
            in_app: Some(true),
            ..Frame::default()
        });
    }
    Stacktrace {
        frames,
        ..Stacktrace::default()
    }
}

fn frame_is_code(f: &Frame) -> bool {
    if f.in_app == Some(true) {
        return true;
    }
    f.function
        .as_deref()
        .is_some_and(|n| CODE_PREFIXES.iter().any(|p| function_has_prefix(n, p)))
}

fn function_has_prefix(func: &str, prefix: &str) -> bool {
    func == prefix
        || func.starts_with(&format!("{prefix}::"))
        || func.starts_with(&format!("{prefix}."))
}

fn scrub_frame(mut f: Frame) -> Frame {
    f.abs_path = None;
    f.vars.clear();
    f.pre_context.clear();
    f.post_context.clear();
    f.context_line = None;
    if let Some(path) = f.filename.take() {
        f.filename = Some(file_name(&path).to_owned());
    }
    f.in_app = Some(true);
    f
}

fn file_name(path: &str) -> &str {
    Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(path)
}

fn non_empty(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

fn env_if(key: &str, ok: fn(&str) -> bool) -> Option<String> {
    let v = non_empty(key)?;
    ok(&v).then_some(v)
}

fn is_ident(s: &str, dash: bool) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || (dash && b == b'-'))
}

fn is_fault_ident(s: &str) -> bool {
    sanitize_fault_ident(s).as_deref() == Some(s)
}

fn sanitize_fault_ident(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() || s.len() > 96 {
        return None;
    }
    let ok = s.bytes().all(|b| {
        b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'.' || b == b'-'
    }) && s
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    ok.then(|| s.to_owned())
}

fn is_location(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 200
        && !s.contains('/')
        && !s.contains('\\')
        && s.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
}

fn is_static_panic_message(m: &str) -> bool {
    let rest = m.strip_prefix("panic: ").unwrap_or(m);
    !rest.is_empty()
        && rest.len() <= 120
        && rest.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
        && !rest.contains('/')
        && !rest.contains('\\')
}

#[derive(Default)]
struct FaultVisitor {
    fault_code: Option<String>,
}

impl Visit for FaultVisitor {
    fn record_str(&mut self, f: &Field, value: &str) {
        if matches!(f.name(), "fault_code" | "error_code") {
            if let Some(code) = sanitize_fault_ident(value) {
                self.fault_code = Some(code);
            }
        }
    }
    fn record_debug(&mut self, f: &Field, value: &dyn std::fmt::Debug) {
        if matches!(f.name(), "fault_code" | "error_code") {
            let s = format!("{value:?}");
            let s = s.trim_matches('"');
            if let Some(code) = sanitize_fault_ident(s) {
                self.fault_code = Some(code);
            }
        }
    }
    fn record_i64(&mut self, _: &Field, _: i64) {}
    fn record_u64(&mut self, _: &Field, _: u64) {}
    fn record_bool(&mut self, _: &Field, _: bool) {}
    fn record_f64(&mut self, _: &Field, _: f64) {}
}
