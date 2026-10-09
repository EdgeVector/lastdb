//! `lastdbd` — the LastDB Mini semantic daemon.
//!
//! Boots the core database (schema declare/query/mutate, app-identity
//! surface, cloud sync when configured) and serves the owner Unix socket at
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

use clap::Parser;
use lastdb_uds::uds::UdsSocket;
use lastdb_uds::uds_router;
use lastdb_uds::worker_pool::UdsWorkerPool;

use lastdb_node::cloud;
use lastdb_node::host::{self, Host};

use lastdbd::cli::{Cli, Command};
use lastdbd::serve::{Server, Surface};
use lastdbd::{conflict_fold, footprint_proof, session, shutdown, tracing_init};

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
    // Product write fence: atom field content size (not a blob store).
    // Docs: fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md
    tracing::info!(
        max_atom_content_bytes = fold_db::atom::max_atom_content_bytes(),
        default = fold_db::atom::DEFAULT_MAX_ATOM_CONTENT_BYTES,
        absolute_max = fold_db::atom::ABSOLUTE_MAX_ATOM_CONTENT_BYTES,
        env = fold_db::atom::MAX_ATOM_CONTENT_BYTES_ENV,
        "atom content size limit (LASTDB_MAX_ATOM_CONTENT_BYTES; large payloads → file-blob/CAS)"
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

fn main() -> Result<(), String> {
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

    tracing::info!(
        target: "lastdbd::allocator",
        allocator = allocator_tuning.name,
        purge_delay_ms = ?allocator_tuning.purge_delay_ms,
        purge_delay_from_env = allocator_tuning.purge_delay_from_env,
        "configured process allocator"
    );

    let cli = Cli::parse();

    let command = cli.command;
    let socket_path = cli.socket_path;
    let full_socket_path = cli.full_socket_path;
    if let Some(Command::ServiceHome { action }) = &command {
        println!("{}", lastdbd::cli::run_service_home(action)?);
        return Ok(());
    }

    let home = host::resolve_home(cli.data_dir)?;
    let data_dir = home.join("data");
    let _footprint_proof_pin = footprint_proof::footprint_proof_pin_from_env()?;
    let resident_key_cap = fold_db::resident::init_resident_key_cap_from_env()?;
    if resident_key_cap != fold_db::resident::RESIDENT_KEY_CAP {
        eprintln!(
            "lastdbd: {}={resident_key_cap} (test cap; default {})",
            fold_db::resident::RESIDENT_KEY_CAP_ENV,
            fold_db::resident::RESIDENT_KEY_CAP
        );
    }

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
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_park(lastdb_node::allocator::collect_thread_heap_if_requested)
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))?;

    if let Some(Command::Connect {
        env,
        api_url,
        invite_code,
        force,
    }) = command
    {
        let url = lastdbd::cli::connect_api_url(api_url, env.as_deref())?;
        return runtime.block_on(cloud::connect(&home, &url, invite_code.as_deref(), force));
    }

    // Schema resolve/register must go to the same environment the home syncs
    // to (papercut-lastdbd-connect-env-dev-still-uses-prod-schema-service).
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
    let sample_interval = lastdb_node::self_metrics::sample_interval_from_env();
    let (sampler_task, home_fold_task) = {
        let _runtime_guard = runtime.enter();
        (
            lastdb_node::self_metrics::spawn_sampler(
                Arc::clone(&host),
                sample_interval,
                Arc::clone(&shutdown_flag),
            ),
            conflict_fold::spawn_home_conflict_fold(Arc::clone(&host), Arc::clone(&shutdown_flag)),
        )
    };

    let socket = UdsSocket::bind_at(
        socket_path.unwrap_or_else(|| data_dir.join(lastdb_uds::uds::SOCKET_FILE_NAME)),
    )
    .map_err(|e| {
        note_fatal(format!(
            "failed to bind socket in {}: {e}",
            data_dir.display()
        ))
    })?;
    println!("lastdbd serving {}", socket.path().display());
    println!("  Home:   {}", home.display());
    println!("  Owner:  {}", host.user_hash);

    register_shutdown_signals(&shutdown_flag).map_err(&note_fatal)?;

    let owner_uid = unsafe { libc::getuid() };
    let worker_pool = UdsWorkerPool::from_env();
    host.attach_uds_workers(worker_pool.clone());
    log_startup_limits(&worker_pool);
    let server = Server {
        host: Arc::clone(&host),
        handle: runtime.handle().clone(),
        worker_pool: worker_pool.clone(),
    };

    // Full-surface setup socket: same data surface PLUS the setup verbs
    // first-time clients need (`POST /api/schemas/load` — fkanban init /
    // fbrain bootstrap probe `<data>/folddb-full.sock` for schema loads, as
    // they do against the full node). Same owner-only peer-cred gate as the
    // narrow socket.
    let full_socket =
        UdsSocket::bind_at(full_socket_path.unwrap_or_else(|| data_dir.join("folddb-full.sock")))
            .map_err(|e| note_fatal(format!("failed to bind full-surface socket: {e}")))?;
    let full_accept = {
        let server = server.clone();
        let shutdown_flag = Arc::clone(&shutdown_flag);
        std::thread::spawn(move || {
            server
                .serve(Surface::Full, &full_socket, owner_uid, &shutdown_flag)
                .map_err(|error| format!("full-surface accept loop failed: {error}"))
        })
    };

    let accept_result = server.serve(Surface::Owner, &socket, owner_uid, &shutdown_flag);
    shutdown_flag.store(true, Ordering::Release);
    let mut shutdown_errors = Vec::new();
    if let Err(error) = accept_result {
        shutdown_errors.push(format!("owner accept loop failed: {error}"));
    }

    // The accept loop returned — a graceful SIGTERM/SIGINT shutdown.
    shutdown::record_shutdown_intent(session_ledger.is_some(), &home, pid, &mut shutdown_errors);

    match full_accept.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => shutdown_errors.push(error),
        Err(_) => shutdown_errors.push("full-surface accept thread panicked".to_string()),
    }
    if let Err(error) = worker_pool.close_and_drain(shutdown::shutdown_handler_drain_timeout()) {
        shutdown_errors.push(format!("request handlers did not drain: {error}"));
    }
    if let Err(error) = shutdown::drain_background(&runtime, &host, sampler_task, home_fold_task) {
        shutdown_errors.push(error);
    }

    // The final flush follows every accepted request and node writer.
    if let Err(error) = runtime.block_on(host.db.shutdown()) {
        shutdown_errors.push(format!("FoldDB shutdown flush failed: {error}"));
    }
    if let Some(heartbeat) = session_heartbeat {
        if let Err(error) = heartbeat.stop() {
            shutdown_errors.push(format!("session heartbeat did not stop: {error}"));
        }
    }
    let start_ts = session_ledger
        .as_ref()
        .map_or(0, |ledger| ledger.own_record().start_ts);
    let shutdown_result = shutdown::complete_shutdown_proof(&home, pid, start_ts, &shutdown_errors);
    shutdown_runtime::finish(runtime, shutdown_result)?;

    println!("lastdbd: shutdown signal received, exiting");
    Ok(())
}
