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
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    socket_path: Option<PathBuf>,
    #[arg(long)]
    enable_search: bool,
}

struct PrivateFormat;

#[derive(Default)]
struct Metrics(Vec<(&'static str, u64)>);

impl Visit for Metrics {
    fn record_u64(&mut self, f: &Field, v: u64) {
        if matches!(
            f.name(),
            "workers" | "queue_capacity" | "max_field_value_bytes" | "default" | "absolute_max"
        ) {
            self.0.push((f.name(), v));
        }
    }
    fn record_i64(&mut self, f: &Field, v: i64) {
        if let Ok(v) = u64::try_from(v) {
            self.record_u64(f, v);
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn fmt::Debug) {}
}

impl<S, N> FormatEvent<S, N> for PrivateFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut w: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let cat = match meta.target() {
            "lastdbd::shutdown" => "shutdown",
            "lastdb_node::cloud" => "cloud",
            "observability::crash" => "panic_report",
            _ => "daemon",
        };
        write!(w, "timestamp={ts} severity={} category={cat}", meta.level())?;
        if meta.target() == "lastdbd" {
            let mut m = Metrics::default();
            event.record(&mut m);
            for (k, v) in m.0 {
                write!(w, " {k}={v}")?;
            }
        }
        writeln!(w)
    }
}

fn init_tracing() -> Option<observability::layers::error::SentryGuard> {
    use observability::layers::error::{build_error_layer, OBS_SENTRY_RELEASE_ENV};
    std::env::set_var(
        OBS_SENTRY_RELEASE_ENV,
        lastdb_node::version::build_version(),
    );
    let filter = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| tracing_subscriber::EnvFilter::try_new(v.trim()).ok())
        .unwrap_or_else(|| tracing_subscriber::EnvFilter::new("info"));
    let (err_layer, guard) = build_error_layer().unzip();
    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .event_format(PrivateFormat);
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(err_layer)
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
        if let Some(p) = fd_regular_file_path(fd) {
            std::thread::spawn(move || loop {
                let _ = rotate_fd_if_needed(fd, &p, max_bytes, keep);
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
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("lastdbd.log");
    let rot = |idx: usize| path.with_file_name(format!("{name}.{idx}"));
    if keep > 0 {
        if let Err(e) = std::fs::remove_file(rot(keep)) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(e);
            }
        }
        for i in (2..=keep).rev().filter(|&i| rot(i - 1).exists()) {
            std::fs::rename(rot(i - 1), rot(i))?;
        }
        std::fs::copy(path, rot(1))?;
        std::fs::set_permissions(rot(1), meta.permissions())?;
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
            // SAFETY: F_GETPATH writes a NUL-terminated path into `buf` on success.
            let cstr = unsafe { CStr::from_ptr(buf.as_ptr()) };
            let p = PathBuf::from(cstr.to_string_lossy().into_owned());
            if std::fs::metadata(&p).is_ok_and(|m| m.is_file()) {
                return Some(p);
            }
        }
    }
    [format!("/dev/fd/{fd}"), format!("/proc/self/fd/{fd}")]
        .into_iter()
        .filter_map(|c| std::fs::read_link(c).ok())
        .find(|p| std::fs::metadata(p).is_ok_and(|m| m.is_file()))
}

fn serve_node(
    runtime: tokio::runtime::Runtime,
    home: &Path,
    socket_path: Option<PathBuf>,
) -> Result<(), String> {
    use fold_db::content_limit::{self as cl, MAX_FIELD_VALUE_BYTES_ENV};
    let (data_dir, pid) = (home.join("data"), std::process::id());
    let host = Arc::new(runtime.block_on(Host::boot(home))?);
    let shutdown = Arc::new(AtomicBool::new(false));
    let socket =
        UdsSocket::bind_at(socket_path.unwrap_or_else(|| data_dir.join(uds::SOCKET_FILE_NAME)))
            .map_err(|e| format!("failed to bind socket in {}: {e}", data_dir.display()))?;
    println!("lastdbd serving the owner socket");
    for sig in [libc::SIGTERM, libc::SIGINT] {
        let f = Arc::clone(&shutdown);
        // SAFETY: the handler only performs an async-signal-safe atomic store.
        unsafe { signal_hook_registry::register(sig, move || f.store(true, Ordering::Relaxed)) }
            .map_err(|e| format!("failed to register signal handler: {e}"))?;
    }
    let pool = UdsWorkerPool::from_env();
    let _ = host.uds_workers.set(pool.clone());
    let s = pool.snapshot();
    tracing::info!(
        workers = s.workers,
        queue_capacity = s.queue_capacity,
        "uds worker pool enabled (LASTDB_UDS_WORKERS / LASTDB_UDS_QUEUE)"
    );
    cl::set_max_field_value_bytes(cl::parse_max_field_value_bytes(
        std::env::var(MAX_FIELD_VALUE_BYTES_ENV).ok(),
    ));
    tracing::info!(
        max_field_value_bytes = cl::max_field_value_bytes(),
        default = cl::DEFAULT_MAX_FIELD_VALUE_BYTES,
        absolute_max = cl::ABSOLUTE_MAX_FIELD_VALUE_BYTES,
        env = MAX_FIELD_VALUE_BYTES_ENV,
        "peer field value size limit"
    );

    let server = lastdbd::Server::new(Arc::clone(&host), runtime.handle().clone(), pool.clone());
    let tasks = lastdbd::spawn_background(&runtime, &host, &shutdown, socket.path());
    let accept_err = server
        .serve(&socket, unsafe { libc::getuid() }, &shutdown)
        .map_err(|e| format!("owner accept loop failed: {e}"));
    shutdown.store(true, Ordering::Release);

    let errs: Vec<String> = [
        accept_err.err(),
        pool.close_and_drain(Duration::from_secs(180))
            .err()
            .map(|e| format!("request handlers did not drain: {e}")),
        lastdbd::drain_background(&runtime, tasks).err(),
        runtime.block_on(host.shutdown_received_databases()).err(),
        runtime
            .block_on(host.shutdown_owned_databases())
            .err()
            .map(|e| format!("owned database shutdown flush failed: {e}")),
        runtime
            .block_on(host.db.shutdown())
            .err()
            .map(|e| format!("FoldDB shutdown flush failed: {e}")),
    ]
    .into_iter()
    .flatten()
    .collect();
    let res = if errs.is_empty() {
        host::write_shutdown_flush_receipt(home, pid, host.process_start_ts)
            .map_err(|e| format!("could not write shutdown flush proof: {e}"))
    } else {
        Err(format!(
            "shutdown did not prove a complete flush: {}",
            errs.join("; ")
        ))
    };
    runtime.shutdown_timeout(Duration::from_secs(1));
    res?;
    println!("lastdbd: shutdown signal received, exiting");
    Ok(())
}

fn run() -> Result<(), String> {
    spawn_stdio_log_rotators();
    let _sentry = init_tracing();
    let cli = Cli::try_parse().unwrap_or_else(|e| {
        if matches!(
            e.kind(),
            clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
        ) {
            e.exit();
        }
        let _ = writeln!(std::io::stderr().lock(), "lastdbd: invalid_arguments");
        std::process::exit(2);
    });
    if cli.enable_search {
        fold_db::db_operations::search_index::queue_policy::enable_for_process();
    }
    let home = host::resolve_home(cli.data_dir)?;
    let warm = fold_db::resident::init_core_warm_bytes_from_env()?;
    if warm != fold_db::resident::DEFAULT_CORE_WARM_BYTES {
        eprintln!(
            "lastdbd: {}={warm} (default {})",
            fold_db::resident::CORE_WARM_BYTES_ENV,
            fold_db::resident::DEFAULT_CORE_WARM_BYTES
        );
    }
    if cli.socket_path.is_none() {
        uds::preflight_data_dir(&home.join("data"))
            .map_err(|e| format!("cannot serve from {}: {e}", home.display()))?;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(
            fold_db::env_flag::var_or("RUST_MIN_STACK", 0usize).max(64 * 1024 * 1024),
        )
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
