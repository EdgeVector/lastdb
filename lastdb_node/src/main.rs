//! `lastdbd` — the LastDB Mini semantic daemon.
//!
//! Boots the core database (schema declare/query/mutate, app-identity
//! surface, explicit byte backups) and serves the owner Unix socket at
//! `<home>/data/folddb.sock` — the same socket, route allowlist, and wire
//! shapes fbrain/fkanban already speak to the full desktop node. No desktop
//! UI, no ingestion, no people-discovery: this crate has no `fold_db_node`
//! dependency, so those subsystems are structurally absent, not disabled.
//!
//! `brew services start lastdb` runs this binary; SIGTERM stops it
//! gracefully (the socket file is unlinked on drop). When brew/launchd cannot
//! pass `--data-dir`, `lastdb admin service-home set <path>` persists the home that
//! service starts should use without baking a personal path into the formula.

mod lastdbd;
mod shutdown_runtime;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use lastdb_node::lastdb_uds::uds::UdsSocket;
use lastdb_node::lastdb_uds::uds_router;
use lastdb_node::lastdb_uds::worker_pool::UdsWorkerPool;

use lastdb_node::cloud;
use lastdb_node::host::{self, Host};

use lastdbd::cli::Cli;
use lastdbd::node_run::{build_runtime, NodeRun};
use lastdbd::serve::Server;
use lastdbd::{background, shutdown, tracing_init};

fn log_startup_limits(worker_pool: &UdsWorkerPool) {
    // Primary concurrency control: bounded worker pool + queue (CPU-shaped).
    // Accept no longer spawns unbounded OS threads (EMFILE incident 2026-07-15).
    // When the queue is full, peers get immediate 503 backpressure
    // (`uds_worker_queue_full`) instead of a "max connections" story.
    // Handler work is still budgeted via exec::block_on_route
    // (LASTDB_UDS_HANDLER_TIMEOUT_SECS). Env: LASTDB_UDS_WORKERS, LASTDB_UDS_QUEUE.
    tracing::info!(
        workers = worker_pool.workers(),
        queue_capacity = worker_pool.queue_capacity(),
        "uds worker pool enabled (LASTDB_UDS_WORKERS / LASTDB_UDS_QUEUE)"
    );
    // Status reports the configured peer field value limit (default 256 KiB).
    // No write path reads it. The structured field keeps its `max_field_value_bytes`
    // name: the private diagnostics log allowlists it by that name.
    // Read the env once here and pass the usize in. Core does not read it.
    install_field_value_limit();
    tracing::info!(
        max_field_value_bytes = fold_db::content_limit::max_field_value_bytes(),
        default = fold_db::content_limit::DEFAULT_MAX_FIELD_VALUE_BYTES,
        absolute_max = fold_db::content_limit::ABSOLUTE_MAX_FIELD_VALUE_BYTES,
        env = fold_db::content_limit::MAX_FIELD_VALUE_BYTES_ENV,
        "peer field value size limit (LASTDB_MAX_FIELD_VALUE_BYTES; growing lists → one record per entry; large opaque bytes → the record's file)"
    );
}

/// Graceful shutdown: SIGTERM/SIGINT set the flag; the accept loop exits on
/// its next poll and the socket file is unlinked on drop.
fn register_shutdown_signals(shutdown: &Arc<AtomicBool>) -> Result<(), String> {
    for signal in [libc::SIGTERM, libc::SIGINT] {
        let flag = Arc::clone(shutdown);
        // SAFETY: the handler only performs an atomic store, which is
        // async-signal-safe.
        unsafe {
            signal_hook_registry::register(signal, move || {
                flag.store(true, Ordering::Relaxed);
            })
        }
        .map_err(|e| format!("failed to register signal handler: {e}"))?;
    }
    Ok(())
}

/// Everything the graceful-shutdown sequence needs after the owner accept loop
/// returns.
struct ShutdownInputs<'a> {
    home: &'a std::path::Path,
    pid: u32,
    runtime: &'a tokio::runtime::Runtime,
    host: &'a Arc<Host>,
    worker_pool: &'a UdsWorkerPool,
    ttl_sweep_task: tokio::task::JoinHandle<()>,
    automatic_backup_task: tokio::task::JoinHandle<()>,
    shared_sync_task: tokio::task::JoinHandle<()>,
    received_shares_task: tokio::task::JoinHandle<()>,
}

/// The accept loop returned (a graceful SIGTERM/SIGINT shutdown): stop the
/// accept loop, drain handlers and background tasks, flush, and write the
/// shutdown proof. Errors are collected so every step still runs.
fn drain_and_prove_shutdown(
    inputs: ShutdownInputs<'_>,
    accept_result: std::io::Result<()>,
) -> Result<(), String> {
    let ShutdownInputs {
        home,
        pid,
        runtime,
        host,
        worker_pool,
        ttl_sweep_task,
        automatic_backup_task,
        shared_sync_task,
        received_shares_task,
    } = inputs;
    let mut shutdown_errors = Vec::new();
    if let Err(error) = accept_result {
        shutdown_errors.push(format!("owner accept loop failed: {error}"));
    }

    host.byte_backup.begin_shutdown();

    if let Err(error) = worker_pool.close_and_drain(shutdown::shutdown_handler_drain_timeout()) {
        shutdown_errors.push(format!("request handlers did not drain: {error}"));
    }
    if let Err(error) = shutdown::drain_background(
        runtime,
        host,
        ttl_sweep_task,
        automatic_backup_task,
        shared_sync_task,
        received_shares_task,
    ) {
        shutdown_errors.push(error);
    }

    // The final flush follows every accepted request and node writer.
    if let Err(error) = shutdown::flush_database(runtime, host) {
        shutdown_errors.push(error);
    }
    shutdown::complete_shutdown_proof(home, pid, host.process_start_ts, &shutdown_errors)
}

/// Parse `LASTDB_MAX_FIELD_VALUE_BYTES` once and pass the usize into core.
fn install_field_value_limit() {
    let raw = std::env::var(fold_db::content_limit::MAX_FIELD_VALUE_BYTES_ENV).ok();
    let limit = fold_db::content_limit::parse_max_field_value_bytes(raw);
    fold_db::content_limit::set_max_field_value_bytes(limit);
}

fn init_core_warm_bytes() -> Result<(), String> {
    let warm_bytes = fold_db::resident::init_core_warm_bytes_from_env()?;
    if warm_bytes != fold_db::resident::DEFAULT_CORE_WARM_BYTES {
        eprintln!(
            "lastdbd: {}={warm_bytes} (default {})",
            fold_db::resident::CORE_WARM_BYTES_ENV,
            fold_db::resident::DEFAULT_CORE_WARM_BYTES
        );
    }
    Ok(())
}

/// Boot the host, bind the owner socket, serve until shutdown, then drain.
fn serve_node(run: NodeRun) -> Result<(), String> {
    let NodeRun {
        runtime,
        home,
        data_dir,
        pid,
        socket_path,
        share,
    } = run;

    let host = runtime.block_on(Host::boot(&home))?;
    let host = Arc::new(host);
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

    let (ttl_sweep_task, automatic_backup_task, shared_sync_task, received_shares_task) =
        background::spawn(&runtime, &host, &shutdown_flag, share)?;

    let accept_result = server.serve(&socket, owner_uid, &shutdown_flag);
    shutdown_flag.store(true, Ordering::Release);
    let shutdown_result = drain_and_prove_shutdown(
        ShutdownInputs {
            home: &home,
            pid,
            runtime: &runtime,
            host: &host,
            worker_pool: &worker_pool,
            ttl_sweep_task,
            automatic_backup_task,
            shared_sync_task,
            received_shares_task,
        },
        accept_result,
    );
    shutdown_runtime::finish(runtime, shutdown_result)?;

    println!("lastdbd: shutdown signal received, exiting");
    Ok(())
}

fn main() -> std::process::ExitCode {
    observability::crash::install_private_panic_hook();
    if run().is_ok() {
        std::process::ExitCode::SUCCESS
    } else {
        use std::io::Write;
        let _ = writeln!(std::io::stderr().lock(), "lastdbd: daemon_failed");
        std::process::ExitCode::FAILURE
    }
}

fn run() -> Result<(), String> {
    lastdb_node::log_rotation::spawn_stdio_log_rotators();

    // Keep launchd-captured stderr logging, and add the DSN-gated ERROR-only
    // Sentry layer without switching Mini to the full node observability file
    // pipeline.
    let _sentry_guard = tracing_init::init_tracing();

    // `GET /api/version` reports the same baked build string as `--version`
    // and Sentry.
    uds_router::set_build_version(lastdb_node::version::build_version());

    let cli = Cli::parse_private();
    if cli.enable_search {
        fold_db::db_operations::search_index::queue_policy::enable_for_process();
    }

    let socket_path = cli.socket_path;

    let home = host::resolve_home(cli.data_dir)?;
    let share = cli
        .share_owner_home
        .as_deref()
        .map(|owner| lastdb_node::share_publication::prepare_automatic(&home, owner))
        .transpose()?;
    let data_dir = home.join("data");
    init_core_warm_bytes()?;

    // Refuse an unservable data dir before any boot work.
    if socket_path.is_none() {
        lastdb_node::lastdb_uds::uds::preflight_data_dir(&data_dir)
            .map_err(|e| format!("cannot serve from {}: {e}", home.display()))?;
    }

    let runtime = build_runtime()?;

    // Schema resolve/register follows the home's saved backup environment.
    cloud::pin_schema_service_environment_from_home(&home);

    let pid = std::process::id();

    serve_node(NodeRun {
        runtime,
        home,
        data_dir,
        pid,
        socket_path,
        share,
    })
}
