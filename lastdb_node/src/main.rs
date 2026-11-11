//! `lastdbd` — the LastDB Mini semantic daemon.

mod lastdbd;

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use lastdb_node::cloud;
use lastdb_node::host::{self, Host};
use lastdb_node::lastdb_uds::uds::UdsSocket;
use lastdb_node::lastdb_uds::uds_router;
use lastdb_node::lastdb_uds::worker_pool::UdsWorkerPool;
use lastdbd::serve::Server;

fn log_startup_limits(worker_pool: &UdsWorkerPool) {
    tracing::info!(
        workers = worker_pool.workers(),
        queue_capacity = worker_pool.queue_capacity(),
        "uds worker pool enabled (LASTDB_UDS_WORKERS / LASTDB_UDS_QUEUE)"
    );
    let raw = std::env::var(fold_db::content_limit::MAX_FIELD_VALUE_BYTES_ENV).ok();
    let limit = fold_db::content_limit::parse_max_field_value_bytes(raw);
    fold_db::content_limit::set_max_field_value_bytes(limit);
    tracing::info!(
        max_field_value_bytes = fold_db::content_limit::max_field_value_bytes(),
        default = fold_db::content_limit::DEFAULT_MAX_FIELD_VALUE_BYTES,
        absolute_max = fold_db::content_limit::ABSOLUTE_MAX_FIELD_VALUE_BYTES,
        env = fold_db::content_limit::MAX_FIELD_VALUE_BYTES_ENV,
        "peer field value size limit"
    );
}

fn register_shutdown_signals(shutdown: &Arc<AtomicBool>) -> Result<(), String> {
    for signal in [libc::SIGTERM, libc::SIGINT] {
        let flag = Arc::clone(shutdown);
        // SAFETY: the handler only performs an async-signal-safe atomic store.
        unsafe {
            signal_hook_registry::register(signal, move || {
                flag.store(true, Ordering::Relaxed);
            })
        }
        .map_err(|e| format!("failed to register signal handler: {e}"))?;
    }
    Ok(())
}

fn serve_node(
    runtime: tokio::runtime::Runtime,
    home: &std::path::Path,
    socket_path: Option<PathBuf>,
    share: Option<lastdb_node::share_publication::AutomaticPublication>,
) -> Result<(), String> {
    let data_dir = home.join("data");
    let pid = std::process::id();
    let host = Arc::new(runtime.block_on(Host::boot(home))?);
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    let socket = UdsSocket::bind_at(
        socket_path
            .unwrap_or_else(|| data_dir.join(lastdb_node::lastdb_uds::uds::SOCKET_FILE_NAME)),
    )
    .map_err(|e| format!("failed to bind socket in {}: {e}", data_dir.display()))?;
    println!("lastdbd serving the owner socket");

    register_shutdown_signals(&shutdown_flag)?;

    let owner_uid = unsafe { libc::getuid() };
    let worker_pool = UdsWorkerPool::from_env();
    host.attach_uds_workers(worker_pool.clone());
    log_startup_limits(&worker_pool);
    let server = Server::new(
        Arc::clone(&host),
        runtime.handle().clone(),
        worker_pool.clone(),
    );

    let tasks = lastdbd::spawn_background(&runtime, &host, &shutdown_flag, share)?;
    let accept_result = server.serve(&socket, owner_uid, &shutdown_flag);
    shutdown_flag.store(true, Ordering::Release);

    let mut shutdown_errors = Vec::new();
    if let Err(error) = accept_result {
        shutdown_errors.push(format!("owner accept loop failed: {error}"));
    }
    host.byte_backup.begin_shutdown();
    if let Err(error) = worker_pool.close_and_drain(Duration::from_secs(180)) {
        shutdown_errors.push(format!("request handlers did not drain: {error}"));
    }
    if let Err(error) = lastdbd::drain_background(&runtime, &host, tasks) {
        shutdown_errors.push(error);
    }
    if let Err(error) = runtime.block_on(host.shutdown_owned_databases()) {
        shutdown_errors.push(format!("owned database shutdown flush failed: {error}"));
    }
    if let Err(error) = runtime.block_on(host.db.shutdown()) {
        shutdown_errors.push(format!("FoldDB shutdown flush failed: {error}"));
    }
    let shutdown_result = if shutdown_errors.is_empty() {
        lastdb_node::shutdown_receipt::write_shutdown_flush_receipt(
            home,
            pid,
            host.process_start_ts,
        )
        .map(|_| ())
        .map_err(|error| format!("could not write shutdown flush proof: {error}"))
    } else {
        Err(format!(
            "shutdown did not prove a complete flush: {}",
            shutdown_errors.join("; ")
        ))
    };

    runtime.shutdown_timeout(Duration::from_secs(1));
    shutdown_result?;
    println!("lastdbd: shutdown signal received, exiting");
    Ok(())
}

fn run() -> Result<(), String> {
    lastdb_node::log_rotation::spawn_stdio_log_rotators();
    let _sentry_guard = lastdbd::init_tracing();
    uds_router::set_build_version(lastdb_node::version::build_version());

    let cli = lastdbd::Cli::parse_private();
    if cli.enable_search {
        fold_db::db_operations::search_index::queue_policy::enable_for_process();
    }

    let home = host::resolve_home(cli.data_dir)?;
    let share = cli
        .share_owner_home
        .as_deref()
        .map(|owner| lastdb_node::share_publication::prepare_automatic(&home, owner))
        .transpose()?;
    let warm_bytes = fold_db::resident::init_core_warm_bytes_from_env()?;
    if warm_bytes != fold_db::resident::DEFAULT_CORE_WARM_BYTES {
        eprintln!(
            "lastdbd: {}={warm_bytes} (default {})",
            fold_db::resident::CORE_WARM_BYTES_ENV,
            fold_db::resident::DEFAULT_CORE_WARM_BYTES
        );
    }
    if cli.socket_path.is_none() {
        lastdb_node::lastdb_uds::uds::preflight_data_dir(&home.join("data"))
            .map_err(|e| format!("cannot serve from {}: {e}", home.display()))?;
    }
    let runtime = lastdbd::build_runtime()?;
    cloud::pin_schema_service_environment_from_home(&home);
    serve_node(runtime, &home, cli.socket_path, share)
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
