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
//! pass `--data-dir`, `lastdbd service-home set <path>` persists the home that
//! service starts should use without baking a personal path into the formula.

mod lastdbd;
mod shutdown_runtime;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use lastdb_uds::uds::UdsSocket;
use lastdb_uds::uds_router;
use lastdb_uds::worker_pool::UdsWorkerPool;

use lastdb_node::cloud;
use lastdb_node::host::{self, Host};

use lastdbd::cli::{Cli, Command};
use lastdbd::node_run::{build_runtime, NodeRun};
use lastdbd::serve::{Server, Surface};
use lastdbd::{background, footprint_proof, session, shutdown, tracing_init};

#[cfg(feature = "purging-allocator")]
#[global_allocator]
static GLOBAL_ALLOCATOR: lastdb_node::allocator::TrackedMiMalloc =
    lastdb_node::allocator::TrackedMiMalloc;

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
    // Product write fence: atom field content size (default 256 KiB).
    // Docs: fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md
    tracing::info!(
        max_atom_content_bytes = fold_db::atom::max_atom_content_bytes(),
        default = fold_db::atom::DEFAULT_MAX_ATOM_CONTENT_BYTES,
        absolute_max = fold_db::atom::ABSOLUTE_MAX_ATOM_CONTENT_BYTES,
        env = fold_db::atom::MAX_ATOM_CONTENT_BYTES_ENV,
        "atom content size limit (LASTDB_MAX_ATOM_CONTENT_BYTES; growing lists → one record per entry; large opaque bytes → file blob/CAS)"
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
    session_ledger: Option<&'a lastdb_node::session_ledger::Ledger>,
    session_heartbeat: Option<lastdb_node::session_ledger::SessionHeartbeat>,
    worker_pool: &'a UdsWorkerPool,
    full_accept: std::thread::JoinHandle<Result<(), String>>,
    sampler_task: tokio::task::JoinHandle<()>,
    home_fold_task: tokio::task::JoinHandle<()>,
    automatic_backup_task: tokio::task::JoinHandle<()>,
}

/// The accept loop returned (a graceful SIGTERM/SIGINT shutdown): stop the
/// accept threads, drain handlers and background tasks, flush, and write the
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
        session_ledger,
        session_heartbeat,
        worker_pool,
        full_accept,
        sampler_task,
        home_fold_task,
        automatic_backup_task,
    } = inputs;
    let mut shutdown_errors = Vec::new();
    if let Err(error) = accept_result {
        shutdown_errors.push(format!("owner accept loop failed: {error}"));
    }

    shutdown::record_shutdown_intent(session_ledger.is_some(), home, pid, &mut shutdown_errors);
    host.byte_backup.begin_shutdown();

    match full_accept.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => shutdown_errors.push(error),
        Err(_) => shutdown_errors.push("full-surface accept thread panicked".to_string()),
    }
    if let Err(error) = worker_pool.close_and_drain(shutdown::shutdown_handler_drain_timeout()) {
        shutdown_errors.push(format!("request handlers did not drain: {error}"));
    }
    if let Err(error) = shutdown::drain_background(
        runtime,
        host,
        sampler_task,
        home_fold_task,
        automatic_backup_task,
    ) {
        shutdown_errors.push(error);
    }

    // The final flush follows every accepted request and node writer.
    if let Err(error) = shutdown::flush_database(runtime, host) {
        shutdown_errors.push(error);
    }
    if let Some(heartbeat) = session_heartbeat {
        if let Err(error) = heartbeat.stop() {
            shutdown_errors.push(format!("session heartbeat did not stop: {error}"));
        }
    }
    let start_ts = session_ledger.map_or(0, |ledger| ledger.own_record().start_ts);
    shutdown::complete_shutdown_proof(home, pid, start_ts, &shutdown_errors)
}

fn log_allocator_tuning(tuning: &lastdb_node::allocator::AllocatorTuning) {
    tracing::info!(
        target: "lastdbd::allocator",
        allocator = tuning.name,
        purge_delay_ms = ?tuning.purge_delay_ms,
        purge_delay_from_env = tuning.purge_delay_from_env,
        "configured process allocator"
    );
}

fn init_resident_key_cap() -> Result<(), String> {
    if std::env::var_os(fold_db::resident::RETIRED_KEY_CAP_ENV).is_some() {
        eprintln!(
            "lastdbd: {} is retired and ignored; the warm set is limited by {}",
            fold_db::resident::RETIRED_KEY_CAP_ENV,
            fold_db::resident::CORE_WARM_BYTES_ENV
        );
    }
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

/// Serve the full-surface setup socket on its own accept thread.
fn spawn_full_surface_accept(
    server: &Server,
    full_socket: UdsSocket,
    owner_uid: u32,
    shutdown_flag: &Arc<AtomicBool>,
) -> std::thread::JoinHandle<Result<(), String>> {
    let server = server.clone();
    let shutdown_flag = Arc::clone(shutdown_flag);
    std::thread::spawn(move || {
        server
            .serve(Surface::Full, &full_socket, owner_uid, &shutdown_flag)
            .map_err(|error| format!("full-surface accept loop failed: {error}"))
    })
}

/// Boot the host, bind both sockets, serve until shutdown, then drain.
fn serve_node(run: NodeRun) -> Result<(), String> {
    let NodeRun {
        runtime,
        home,
        data_dir,
        pid,
        session_ledger,
        session_heartbeat,
        socket_path,
        full_socket_path,
    } = run;
    let note_fatal =
        |err: String| -> String { session::note_fatal(session_ledger.is_some(), &home, pid, err) };

    let host = Arc::new(runtime.block_on(Host::boot(&home)).map_err(&note_fatal)?);
    // Capture this process's own boot identity once, from the ledger entry
    // this same process just wrote — not from a re-read of the shared
    // per-home ledger file, which a different process sharing this home can
    // append a newer row to after this point
    // (papercut-lastdb-primary-boot-identity-stale-phantom-pid-20260927).
    if let Some(ledger) = &session_ledger {
        let _ = host.own_boot_identity.set(ledger.own_record());
    }
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    let socket = UdsSocket::bind_at(
        socket_path.unwrap_or_else(|| data_dir.join(lastdb_uds::uds::SOCKET_FILE_NAME)),
    )
    .map_err(|e| {
        note_fatal(format!(
            "failed to bind socket in {}: {e}",
            data_dir.display()
        ))
    })?;
    println!("lastdbd serving the owner socket");

    register_shutdown_signals(&shutdown_flag).map_err(&note_fatal)?;

    let owner_uid = unsafe { libc::getuid() };
    let worker_pool = UdsWorkerPool::from_env();
    host.attach_uds_workers(worker_pool.clone());
    log_startup_limits(&worker_pool);
    let server = Server::new(
        Arc::clone(&host),
        runtime.handle().clone(),
        worker_pool.clone(),
    );

    // Full-surface setup socket: same data surface PLUS the setup verbs
    // first-time clients need (`POST /api/schemas/load` — fkanban init /
    // fbrain bootstrap probe `<data>/folddb-full.sock` for schema loads, as
    // they do against the full node). Same owner-only peer-cred gate as the
    // narrow socket.
    let full_socket =
        UdsSocket::bind_at(full_socket_path.unwrap_or_else(|| data_dir.join("folddb-full.sock")))
            .map_err(|e| note_fatal(format!("failed to bind full-surface socket: {e}")))?;
    let (sampler_task, home_fold_task, automatic_backup_task) =
        background::spawn(&runtime, &host, &shutdown_flag);

    let full_accept = spawn_full_surface_accept(&server, full_socket, owner_uid, &shutdown_flag);

    let accept_result = server.serve(Surface::Owner, &socket, owner_uid, &shutdown_flag);
    shutdown_flag.store(true, Ordering::Release);
    let shutdown_result = drain_and_prove_shutdown(
        ShutdownInputs {
            home: &home,
            pid,
            runtime: &runtime,
            host: &host,
            session_ledger: session_ledger.as_ref(),
            session_heartbeat,
            worker_pool: &worker_pool,
            full_accept,
            sampler_task,
            home_fold_task,
            automatic_backup_task,
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
    let allocator_tuning = lastdb_node::allocator::configure();
    lastdb_node::log_rotation::spawn_stdio_log_rotators();

    // Keep launchd-captured stderr logging, and add the DSN-gated ERROR-only
    // Sentry layer without switching Mini to the full node observability file
    // pipeline. The guard also keeps crash/unclean-exit telemetry flushable.
    let _sentry_guard = tracing_init::init_tracing();

    // `GET /api/version` reports the same baked build string as `--version`,
    // Sentry, and the boot ledger. The transport crate cannot see this crate's
    // `build.rs` stamp, so the binary installs it before any socket serves.
    uds_router::set_build_version(lastdb_node::crash_attribution::build_version());

    log_allocator_tuning(&allocator_tuning);

    let cli = Cli::parse_private();
    if cli.enable_search {
        fold_db::db_operations::search_index::queue_policy::enable_for_process();
    }

    let socket_path = cli.socket_path;
    let full_socket_path = cli.full_socket_path;
    if let Some(Command::ServiceHome { action }) = &cli.command {
        println!("{}", lastdbd::cli::run_service_home(action)?);
        return Ok(());
    }

    let home = host::resolve_home(cli.data_dir)?;
    let data_dir = home.join("data");
    let _footprint_proof_pin = footprint_proof::footprint_proof_pin_from_env()?;
    init_resident_key_cap()?;

    // Refuse an unservable data dir before ANY boot work: the socket-path length
    // check is pure arithmetic, but both binds happen after `Host::boot` and
    // after the session ledger records a start. Failing there burns a full boot
    // and leaves no clean-shutdown record, so the next start misreports a static
    // configuration error as an unclean crash. Cheap check, earliest point.
    if socket_path.is_none() && full_socket_path.is_none() {
        lastdb_uds::uds::preflight_data_dir(&data_dir)
            .map_err(|e| format!("cannot serve from {}: {e}", home.display()))?;
    }

    // Multi-thread runtime: the accept loop itself runs on the main thread;
    // each connection's handler blocks on async core calls via this runtime.
    // Tokio workers collect on park. A UDS worker never parks on this
    // runtime; it collects at request end via `collect_finished_request_heap`.
    let runtime = build_runtime()?;

    // Schema resolve/register follows the home's saved backup environment.
    cloud::pin_schema_service_environment_from_home(&home);

    // A Rust panic => a durable local crash report. Under `panic = "abort"` the
    // hook is the last code to run, so the report lands before the process dies.
    lastdb_node::crash_attribution::install_crash_hook(&home);
    // Heartbeat every ~60s so the NEXT boot can compute downtime even if this
    // session dies uncleanly.
    let pid = std::process::id();
    let session_ledger = session::record_start(&home, pid);
    let session_heartbeat = session_ledger.as_ref().and_then(|ledger| {
        ledger
            .spawn_heartbeat(std::time::Duration::from_secs(60))
            .ok()
    });

    serve_node(NodeRun {
        runtime,
        home,
        data_dir,
        pid,
        session_ledger,
        session_heartbeat,
        socket_path,
        full_socket_path,
    })
}
