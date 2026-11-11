//! `lastdbd` CLI, runtime setup, fixed-field stderr diagnostics, and background tasks.

pub(crate) mod serve;

use std::fmt;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use lastdb_node::host::Host;
use tokio::task::JoinHandle;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

const RUNTIME_STACK_BYTES: usize = 64 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(
    name = "lastdbd",
    about = "LastDB Mini semantic daemon: core DB + app-identity + explicit byte backups over the owner Unix socket",
    version = env!("FOLDDB_BUILD_VERSION")
)]
pub(crate) struct Cli {
    /// Node home directory (default: LASTDB_HOME / service-home / ~/.lastdb).
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,

    /// Publish changes in this share home using the existing owner home.
    #[arg(long, requires = "data_dir")]
    pub(crate) share_owner_home: Option<PathBuf>,

    /// Worker socket path. The default is `<home>/data/folddb.sock`.
    #[arg(long)]
    pub(crate) socket_path: Option<PathBuf>,

    /// Enable bounded change delivery to LastSeek/Search. Off by default.
    #[arg(long)]
    pub(crate) enable_search: bool,
}

impl Cli {
    pub(crate) fn parse_private() -> Self {
        match Self::try_parse() {
            Ok(cli) => cli,
            Err(error) => {
                if matches!(
                    error.kind(),
                    clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
                ) {
                    error.exit();
                }
                let _ = writeln!(std::io::stderr().lock(), "lastdbd: invalid_arguments");
                std::process::exit(2);
            }
        }
    }
}

pub(crate) fn build_runtime() -> Result<tokio::runtime::Runtime, String> {
    let stack = std::env::var("RUST_MIN_STACK")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0)
        .max(RUNTIME_STACK_BYTES);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(stack)
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))
}

struct PrivateFormat;

impl<S, N> FormatEvent<S, N> for PrivateFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let category = match metadata.target() {
            "lastdbd::shutdown" => "shutdown",
            "lastdb_node::cloud" => "cloud",
            "observability::crash" => "panic_report",
            _ => "daemon",
        };
        write!(
            writer,
            "timestamp={timestamp} severity={} category={category}",
            metadata.level()
        )?;
        let mut metrics = Metrics::default();
        if metadata.target() == "lastdbd" {
            event.record(&mut metrics);
        }
        for (name, value) in metrics.values {
            write!(writer, " {name}={value}")?;
        }
        writeln!(writer)
    }
}

#[derive(Default)]
struct Metrics {
    values: Vec<(&'static str, u64)>,
}

impl Visit for Metrics {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if matches!(
            field.name(),
            "workers" | "queue_capacity" | "max_field_value_bytes" | "default" | "absolute_max"
        ) {
            self.values.push((field.name(), value));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if let Ok(value) = u64::try_from(value) {
            self.record_u64(field, value);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

pub(crate) fn init_tracing() -> Option<observability::layers::error::SentryGuard> {
    use observability::layers::error::{build_error_layer, OBS_SENTRY_RELEASE_ENV};

    std::env::set_var(
        OBS_SENTRY_RELEASE_ENV,
        lastdb_node::version::build_version(),
    );
    let directive = std::env::var("RUST_LOG")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty() && tracing_subscriber::EnvFilter::try_new(v).is_ok())
        .unwrap_or_else(|| "info".to_string());
    let env_filter = tracing_subscriber::EnvFilter::try_new(&directive)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .event_format(PrivateFormat);
    let (error_layer, sentry_guard) = build_error_layer().unzip();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(error_layer)
        .init();
    sentry_guard
}

pub(crate) type BackgroundTasks = (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>);

pub(crate) fn spawn_background(
    runtime: &tokio::runtime::Runtime,
    host: &Arc<Host>,
    shutdown: &Arc<AtomicBool>,
    share: Option<lastdb_node::share_publication::AutomaticPublication>,
) -> Result<BackgroundTasks, String> {
    let _runtime_guard = runtime.enter();
    lastdb_node::byte_cloud_backup::register_blob_source(host);
    let backup = if let Some(share) = share {
        share.spawn(Arc::clone(host), Arc::clone(shutdown))?
    } else {
        lastdb_node::byte_cloud_backup::spawn_automatic(Arc::clone(host), Arc::clone(shutdown))
    };
    Ok((
        lastdb_node::jobs::ttl_sweep::spawn_periodic(Arc::clone(host), Arc::clone(shutdown)),
        backup,
        lastdb_node::registered_share::spawn(host.home.clone(), Arc::clone(shutdown)),
    ))
}

pub(crate) fn drain_background(
    runtime: &tokio::runtime::Runtime,
    host: &Arc<Host>,
    (ttl_sweep_task, automatic_backup_task, received_shares_task): BackgroundTasks,
) -> Result<(), String> {
    runtime.block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        let drain = async {
            let (ttl_sweep, automatic, received_shares, byte_backup) = tokio::join!(
                ttl_sweep_task,
                automatic_backup_task,
                received_shares_task,
                host.byte_backup.drain(deadline),
            );
            let mut errors = Vec::new();
            for (name, result) in [
                ("ttl sweep", ttl_sweep),
                ("automatic backup", automatic),
                ("received shares", received_shares),
            ] {
                if let Err(error) = result {
                    errors.push(format!("{name} task failed: {error}"));
                }
            }
            if let Err(error) = byte_backup {
                errors.push(format!("byte backup did not drain: {error}"));
            }
            if errors.is_empty() {
                Ok(())
            } else {
                Err(errors.join("; "))
            }
        };
        tokio::time::timeout_at(deadline, drain)
            .await
            .map_err(|_| "background writers did not drain within 180 seconds".to_string())?
    })
}
