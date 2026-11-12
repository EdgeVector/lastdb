//! `lastdbd` — the LastDB Mini semantic daemon.

mod lastdbd;

#[cfg(target_os = "macos")]
use std::ffi::CStr;
use std::fmt;
use std::io::Write;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use lastdb_node::cloud;
use lastdb_node::host::{self, Host};
use lastdb_node::lastdb_uds::uds::{self, UdsSocket};
use lastdb_node::lastdb_uds::uds_router;
use lastdb_node::lastdb_uds::worker_pool::UdsWorkerPool;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Parser, Debug)]
#[command(
    name = "lastdbd",
    about = "LastDB Mini semantic daemon: core DB + app-identity + signed database sync over the owner Unix socket",
    version = env!("FOLDDB_BUILD_VERSION")
)]
struct Cli {
    /// Node home directory (default: LASTDB_HOME / service-home / ~/.lastdb).
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Worker socket path. The default is `<home>/data/folddb.sock`.
    #[arg(long)]
    socket_path: Option<PathBuf>,
    /// Enable bounded change delivery to LastSeek/Search. Off by default.
    #[arg(long)]
    enable_search: bool,
}

impl Cli {
    fn parse_private() -> Self {
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

struct PrivateFormat;

#[derive(Default)]
struct Metrics(Vec<(&'static str, u64)>);

impl Visit for Metrics {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if matches!(
            field.name(),
            "workers" | "queue_capacity" | "max_field_value_bytes" | "default" | "absolute_max"
        ) {
            self.0.push((field.name(), value));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if let Ok(value) = u64::try_from(value) {
            self.record_u64(field, value);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

impl<S, N> FormatEvent<S, N> for PrivateFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let category = match meta.target() {
            "lastdbd::shutdown" => "shutdown",
            "lastdb_node::cloud" => "cloud",
            "observability::crash" => "panic_report",
            _ => "daemon",
        };
        write!(
            writer,
            "timestamp={ts} severity={} category={category}",
            meta.level()
        )?;
        let mut metrics = Metrics::default();
        if meta.target() == "lastdbd" {
            event.record(&mut metrics);
        }
        for (name, value) in metrics.0 {
            write!(writer, " {name}={value}")?;
        }
        writeln!(writer)
    }
}

fn init_tracing() -> Option<observability::layers::error::SentryGuard> {
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
    let filter = tracing_subscriber::EnvFilter::try_new(&directive)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .event_format(PrivateFormat);
    let (error_layer, guard) = build_error_layer().unzip();
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(error_layer)
        .init();
    guard
}

fn spawn_stdio_log_rotators() {
    if matches!(
        std::env::var("LASTDBD_STDIO_LOG_ROTATION").ok().as_deref(),
        Some("0" | "false" | "FALSE" | "off" | "OFF")
    ) {
        return;
    }
    let max_bytes = fold_db::env_flag::var_or("LASTDBD_STDIO_LOG_MAX_BYTES", 10 * 1024 * 1024u64);
    if max_bytes == 0 {
        return;
    }
    let keep = fold_db::env_flag::var_or("LASTDBD_STDIO_LOG_KEEP", 5usize);
    let interval = Duration::from_secs(
        fold_db::env_flag::var_or("LASTDBD_STDIO_LOG_CHECK_SECONDS", 60u64).max(1),
    );
    for fd in [libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        if let Some(path) = fd_regular_file_path(fd) {
            std::thread::spawn(move || loop {
                let _ = rotate_fd_if_needed(fd, &path, max_bytes, keep);
                std::thread::sleep(interval);
            });
        }
    }
}

fn rotate_fd_if_needed(fd: RawFd, path: &Path, max_bytes: u64, keep: usize) -> std::io::Result<()> {
    let meta = std::fs::metadata(path)?;
    if !meta.is_file() || meta.len() <= max_bytes {
        return Ok(());
    }
    let rotated = |idx: usize| {
        let name = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or("lastdbd.log");
        path.with_file_name(format!("{name}.{idx}"))
    };
    if keep > 0 {
        match std::fs::remove_file(rotated(keep)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        for idx in (2..=keep).rev() {
            let src = rotated(idx - 1);
            if src.exists() {
                std::fs::rename(src, rotated(idx))?;
            }
        }
        let first = rotated(1);
        std::fs::copy(path, &first)?;
        std::fs::set_permissions(first, meta.permissions())?;
    }
    // SAFETY: `fd` is one of this process's stdio descriptors; `ftruncate` and `lseek` do not consume it.
    if unsafe { libc::ftruncate(fd, 0) } != 0 || unsafe { libc::lseek(fd, 0, libc::SEEK_SET) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn fd_regular_file_path(fd: RawFd) -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let mut buf = [0 as libc::c_char; libc::MAXPATHLEN as usize];
        // SAFETY: `buf` is a writable MAXPATHLEN buffer as required by F_GETPATH.
        if unsafe { libc::fcntl(fd, libc::F_GETPATH, buf.as_mut_ptr()) } == 0 {
            let path = PathBuf::from(
                unsafe { CStr::from_ptr(buf.as_ptr()) }
                    .to_string_lossy()
                    .into_owned(),
            );
            if std::fs::metadata(&path).is_ok_and(|m| m.is_file()) {
                return Some(path);
            }
        }
    }
    [format!("/dev/fd/{fd}"), format!("/proc/self/fd/{fd}")]
        .into_iter()
        .filter_map(|c| std::fs::read_link(c).ok())
        .find(|p| std::fs::metadata(p).is_ok_and(|m| m.is_file()))
}

fn log_startup_limits(worker_pool: &UdsWorkerPool) {
    tracing::info!(
        workers = worker_pool.workers(),
        queue_capacity = worker_pool.queue_capacity(),
        "uds worker pool enabled (LASTDB_UDS_WORKERS / LASTDB_UDS_QUEUE)"
    );
    let limit = fold_db::content_limit::parse_max_field_value_bytes(
        std::env::var(fold_db::content_limit::MAX_FIELD_VALUE_BYTES_ENV).ok(),
    );
    fold_db::content_limit::set_max_field_value_bytes(limit);
    tracing::info!(
        max_field_value_bytes = fold_db::content_limit::max_field_value_bytes(),
        default = fold_db::content_limit::DEFAULT_MAX_FIELD_VALUE_BYTES,
        absolute_max = fold_db::content_limit::ABSOLUTE_MAX_FIELD_VALUE_BYTES,
        env = fold_db::content_limit::MAX_FIELD_VALUE_BYTES_ENV,
        "peer field value size limit"
    );
}

fn serve_node(
    runtime: tokio::runtime::Runtime,
    home: &Path,
    socket_path: Option<PathBuf>,
) -> Result<(), String> {
    let data_dir = home.join("data");
    let pid = std::process::id();
    let host = Arc::new(runtime.block_on(Host::boot(home))?);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let socket =
        UdsSocket::bind_at(socket_path.unwrap_or_else(|| data_dir.join(uds::SOCKET_FILE_NAME)))
            .map_err(|e| format!("failed to bind socket in {}: {e}", data_dir.display()))?;
    println!("lastdbd serving the owner socket");

    for signal in [libc::SIGTERM, libc::SIGINT] {
        let flag = Arc::clone(&shutdown_flag);
        // SAFETY: the handler only performs an async-signal-safe atomic store.
        unsafe {
            signal_hook_registry::register(signal, move || flag.store(true, Ordering::Relaxed))
        }
        .map_err(|e| format!("failed to register signal handler: {e}"))?;
    }

    let owner_uid = unsafe { libc::getuid() };
    let worker_pool = UdsWorkerPool::from_env();
    host.attach_uds_workers(worker_pool.clone());
    log_startup_limits(&worker_pool);
    let server = lastdbd::Server::new(
        Arc::clone(&host),
        runtime.handle().clone(),
        worker_pool.clone(),
    );

    let tasks = lastdbd::spawn_background(&runtime, &host, &shutdown_flag, socket.path());
    let accept_result = server.serve(&socket, owner_uid, &shutdown_flag);
    shutdown_flag.store(true, Ordering::Release);

    let mut errors = Vec::new();
    if let Err(e) = accept_result {
        errors.push(format!("owner accept loop failed: {e}"));
    }
    if let Err(e) = worker_pool.close_and_drain(Duration::from_secs(180)) {
        errors.push(format!("request handlers did not drain: {e}"));
    }
    if let Err(e) = lastdbd::drain_background(&runtime, tasks) {
        errors.push(e);
    }
    if let Err(e) = runtime.block_on(host.shutdown_received_databases()) {
        errors.push(e);
    }
    if let Err(e) = runtime.block_on(host.shutdown_owned_databases()) {
        errors.push(format!("owned database shutdown flush failed: {e}"));
    }
    if let Err(e) = runtime.block_on(host.db.shutdown()) {
        errors.push(format!("FoldDB shutdown flush failed: {e}"));
    }
    let result = if errors.is_empty() {
        host::write_shutdown_flush_receipt(home, pid, host.process_start_ts)
            .map_err(|e| format!("could not write shutdown flush proof: {e}"))
    } else {
        Err(format!(
            "shutdown did not prove a complete flush: {}",
            errors.join("; ")
        ))
    };
    runtime.shutdown_timeout(Duration::from_secs(1));
    result?;
    println!("lastdbd: shutdown signal received, exiting");
    Ok(())
}

fn run() -> Result<(), String> {
    spawn_stdio_log_rotators();
    let _sentry_guard = init_tracing();
    uds_router::set_build_version(lastdb_node::version::build_version());
    let cli = Cli::parse_private();
    if cli.enable_search {
        fold_db::db_operations::search_index::queue_policy::enable_for_process();
    }
    let home = host::resolve_home(cli.data_dir)?;
    let warm_bytes = fold_db::resident::init_core_warm_bytes_from_env()?;
    if warm_bytes != fold_db::resident::DEFAULT_CORE_WARM_BYTES {
        eprintln!(
            "lastdbd: {}={warm_bytes} (default {})",
            fold_db::resident::CORE_WARM_BYTES_ENV,
            fold_db::resident::DEFAULT_CORE_WARM_BYTES
        );
    }
    if cli.socket_path.is_none() {
        uds::preflight_data_dir(&home.join("data"))
            .map_err(|e| format!("cannot serve from {}: {e}", home.display()))?;
    }
    let stack = fold_db::env_flag::var_or("RUST_MIN_STACK", 0usize).max(64 * 1024 * 1024);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(stack)
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))?;
    cloud::pin_schema_service_environment_from_home(&home);
    serve_node(runtime, &home, cli.socket_path)
}

fn main() -> std::process::ExitCode {
    observability::crash::install_private_panic_hook();
    if run().is_ok() {
        std::process::ExitCode::SUCCESS
    } else {
        let _ = writeln!(std::io::stderr().lock(), "lastdbd: daemon_failed");
        std::process::ExitCode::FAILURE
    }
}
