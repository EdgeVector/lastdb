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

mod shutdown_runtime;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::{Parser, Subcommand};
use fold_db::access::CallerVerification;
use lastdb_uds::uds::UdsSocket;
use lastdb_uds::uds_http;
use lastdb_uds::uds_router::{self, SocketKind};
use lastdb_uds::worker_pool::{SubmitError, UdsWorkerPool};

use lastdb_node::host::{self, Host};
use lastdb_node::{cloud, exec, service_home};

const FOOTPRINT_PROOF_PIN_BYTES_ENV: &str = "LASTDB_FOOTPRINT_PROOF_PIN_BYTES";

fn shutdown_handler_drain_timeout() -> std::time::Duration {
    #[cfg(debug_assertions)]
    if std::env::var("LASTDB_ISOLATED_COPY").ok().as_deref() == Some("1") {
        if let Ok(raw) = std::env::var("LASTDB_TEST_SHUTDOWN_DRAIN_TIMEOUT_MS") {
            if let Ok(ms) = raw.parse::<u64>() {
                if (1..=180_000).contains(&ms) {
                    return std::time::Duration::from_millis(ms);
                }
            }
        }
    }
    std::time::Duration::from_secs(180)
}

fn parse_footprint_proof_pin_bytes(
    raw: Option<&str>,
    keychain_disabled: bool,
) -> Result<Option<u64>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let requested = raw
        .trim()
        .parse::<u64>()
        .map_err(|error| format!("invalid {FOOTPRINT_PROOF_PIN_BYTES_ENV}={raw:?}: {error}"))?;
    if requested == 0 {
        return Ok(None);
    }
    if !keychain_disabled {
        return Err(format!(
            "{FOOTPRINT_PROOF_PIN_BYTES_ENV} requires FOLDDB_DISABLE_KEYCHAIN=1"
        ));
    }
    Ok(Some(requested))
}

/// Hold a debug-only live allocation for the CoW footprint proof. Release
/// builds reject the knob, and debug builds require keychain-disabled mode so
/// the fixture cannot attach itself to a normal service boot by accident.
fn footprint_proof_pin_from_env() -> Result<Option<Box<[u8]>>, String> {
    let raw = std::env::var(FOOTPRINT_PROOF_PIN_BYTES_ENV).ok();
    let keychain_disabled = std::env::var("FOLDDB_DISABLE_KEYCHAIN").ok().as_deref() == Some("1");
    let Some(requested) = parse_footprint_proof_pin_bytes(raw.as_deref(), keychain_disabled)?
    else {
        return Ok(None);
    };
    #[cfg(not(debug_assertions))]
    {
        let _ = requested;
        Err(format!(
            "{FOOTPRINT_PROOF_PIN_BYTES_ENV} is available only in debug builds"
        ))
    }
    #[cfg(debug_assertions)]
    {
        let bytes = usize::try_from(requested).map_err(|_| {
            format!("{FOOTPRINT_PROOF_PIN_BYTES_ENV} exceeds this platform's address space")
        })?;
        let mut allocation = Vec::new();
        allocation.try_reserve_exact(bytes).map_err(|error| {
            format!("could not reserve {requested} footprint-proof bytes: {error}")
        })?;
        allocation.resize(bytes, 0xA5);
        tracing::warn!(
            bytes = requested,
            "holding debug-only non-reclaimable allocation for the footprint proof"
        );
        Ok(Some(allocation.into_boxed_slice()))
    }
}
#[cfg(feature = "purging-allocator")]
#[global_allocator]
static GLOBAL_ALLOCATOR: lastdb_node::allocator::TrackedMiMalloc =
    lastdb_node::allocator::TrackedMiMalloc;

/// Collect this UDS worker's heap after one request, before the next job.
fn collect_finished_request_heap() {
    lastdb_node::allocator::collect_request_heap_if_over_slack();
}

fn init_tracing() -> Option<observability::layers::error::SentryGuard> {
    use observability::layers::error::build_error_layer;
    use observability::layers::reload::build_reload_layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Registry;

    // The binary's baked version is authoritative for the Sentry release, and
    // it must be installed BEFORE `build_error_layer` binds the client. Any
    // disagreeing operator value is reported after the subscriber exists — a
    // `warn!` emitted here would go nowhere.
    let ignored_release = lastdb_node::crash_attribution::install_baked_sentry_release();

    // The startup directive is resolved exactly as before — RUST_LOG when set,
    // otherwise `info`. It is captured as a string so the runtime control
    // surface can report the filter the process actually booted with;
    // `EnvFilter` itself only round-trips through `Display`, which normalizes
    // an empty directive set to the empty string, so an operator reading
    // `GET /api/system/log-filter` on a default boot sees `info` rather than
    // nothing.
    let initial_directive = std::env::var("RUST_LOG")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .filter(|v| tracing_subscriber::EnvFilter::try_new(v).is_ok())
        .unwrap_or_else(|| "info".to_string());
    let env_filter = tracing_subscriber::EnvFilter::try_new(&initial_directive)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // RELOAD wraps the filter so `POST /api/system/log-filter` can swap it in
    // place. Composition is otherwise byte-identical to the previous plain
    // `.with(env_filter)`: the reload layer is the same global filter, added
    // innermost, exactly as `observability::init_node` composes it.
    let (reload_layer, reload_handle) = build_reload_layer::<Registry>(env_filter);
    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    let (error_layer, sentry_guard) = match build_error_layer() {
        Some((layer, guard)) => (Some(layer), Some(guard)),
        None => (None, None),
    };

    tracing_subscriber::registry()
        .with(reload_layer)
        .with(fmt_layer)
        .with(error_layer)
        .init();

    // Only after the subscriber exists, so a rejected directive later has
    // somewhere to be logged.
    lastdb_node::ops::log_filter::install(reload_handle, initial_directive);

    if let Some(ignored) = ignored_release {
        tracing::warn!(
            ignored_release = %ignored,
            release = %lastdb_node::crash_attribution::build_version(),
            env = %observability::layers::error::OBS_SENTRY_RELEASE_ENV,
            "ignoring a configured Sentry release that disagrees with this binary's \
             baked build version; the baked version is authoritative and the env var \
             can be dropped from the launchd plist"
        );
    }

    sentry_guard
}

#[derive(Parser, Debug)]
#[command(
    name = "lastdbd",
    about = "LastDB Mini semantic daemon: core DB + app-identity + cloud sync over the owner Unix socket",
    // Stamped by build.rs (tag -> git describe -> manifest), matching the
    // release gate's --version-equals-tag assertion.
    version = env!("FOLDDB_BUILD_VERSION")
)]
struct Cli {
    /// Node home directory (default: LASTDB_HOME / FOLDDB_HOME / ~/.lastdb;
    /// an existing ~/.folddb is honored in place).
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Worker socket path. The proxy owns the public socket when this option
    /// points at a private path. The default is `<home>/data/folddb.sock`.
    #[arg(long)]
    socket_path: Option<PathBuf>,

    /// Full-surface worker socket path. The default is
    /// `<home>/data/folddb-full.sock`.
    #[arg(long)]
    full_socket_path: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Persist or inspect the home directory used by brew/launchd service starts.
    ServiceHome {
        #[command(subcommand)]
        action: ServiceHomeCommand,
    },
    /// Connect cloud sync. On a fresh home with --invite-code, creates a new
    /// identity and prints its recovery phrase. Otherwise reads the 24-word
    /// phrase from stdin to join an existing account as another device. Run
    /// while the daemon is stopped; the next boot pulls the account's data.
    Connect {
        /// Exemem environment to register against (dev | prod). Defaults to
        /// the profile's environment resolution (EXEMEM_ENV / build profile).
        #[arg(long)]
        env: Option<String>,
        /// Explicit Exemem API URL (overrides --env).
        #[arg(long)]
        api_url: Option<String>,
        /// Invite code for first-device onboarding on a fresh data dir.
        #[arg(long)]
        invite_code: Option<String>,
        /// Replace an existing DIFFERENT identity.key (orphans data written
        /// under the old key — the old data dir will no longer decrypt).
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ServiceHomeCommand {
    /// Persist the home directory service starts should use.
    Set {
        /// Absolute path, or a path beginning with ~/; must not resolve to ~/.folddb.
        home: PathBuf,
    },
    /// Show the persisted service home, if one is configured.
    Show,
    /// Clear the persisted service home.
    Clear,
}

/// Fold `hcu:evt` into `hcu:mols` when pressure is clear.
///
/// The copy builder runs once at task start. It is a no-op unless
/// `LASTDB_BUILD_CONFLICT_STAMP_ON_COPY` is set, and that flag stays off on a
/// live daemon. The fold does not walk `conflict\0` or `mcc:`.
fn spawn_home_conflict_fold(
    host: Arc<Host>,
    shutdown: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(error) = host
            .db
            .db_ops()
            .build_home_conflict_stamp_on_copy(None)
            .await
        {
            tracing::warn!(
                target: "lastdbd::home_conflict_fold",
                error = %error,
                "home conflict stamp build on copy failed"
            );
        }
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        while !shutdown.load(Ordering::Acquire) {
            tokio::select! {
                _ = interval.tick() => {},
                _ = async {
                    while !shutdown.load(Ordering::Acquire) {
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    }
                } => break,
            }
            if shutdown.load(Ordering::Acquire) {
                break;
            }
            if !lastdb_node::footprint::conflict_fold_pressure_clear() {
                continue;
            }
            if let Err(error) = host.db.db_ops().fold_home_conflict_events(None).await {
                tracing::warn!(
                    target: "lastdbd::home_conflict_fold",
                    error = %error,
                    "home conflict event fold failed"
                );
            }
        }
    })
}

fn complete_shutdown_proof(
    home: &std::path::Path,
    pid: u32,
    start_ts: u64,
    errors: &[String],
) -> Result<(), String> {
    if !errors.is_empty() {
        return Err(format!(
            "shutdown did not prove a complete flush: {}",
            errors.join("; ")
        ));
    }
    lastdb_node::session_ledger::write_shutdown_flush_receipt(home, pid, start_ts)
        .map_err(|error| format!("could not write shutdown flush proof: {error}"))?;
    if let Err(error) = lastdb_node::session_ledger::mark_clean_shutdown_with_reason(
        home,
        pid,
        Some("signal (graceful shutdown)"),
    ) {
        lastdb_node::session_ledger::clear_shutdown_flush_receipt(home)
            .map_err(|clear| format!("could not clear failed shutdown proof: {clear}"))?;
        return Err(format!("could not record clean shutdown: {error}"));
    }
    Ok(())
}

fn main() -> Result<(), String> {
    let allocator_tuning = lastdb_node::allocator::configure();
    lastdb_node::log_rotation::spawn_stdio_log_rotators();

    // Keep launchd-captured stderr logging, and add the DSN-gated ERROR-only
    // Sentry layer without switching Mini to the full node observability file
    // pipeline. The guard also keeps crash/unclean-exit telemetry flushable.
    let _sentry_guard = init_tracing();

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
        let message = match action {
            ServiceHomeCommand::Set { home } => service_home::set_configured_home(home),
            ServiceHomeCommand::Show => service_home::show_configured_home(),
            ServiceHomeCommand::Clear => service_home::clear_configured_home(),
        }?;
        println!("{message}");
        return Ok(());
    }

    let home = host::resolve_home(cli.data_dir)?;
    let data_dir = home.join("data");
    let _footprint_proof_pin = footprint_proof_pin_from_env()?;
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
        let url = match (api_url, env.as_deref()) {
            (Some(url), _) => url,
            (None, Some("dev")) => folddb_profile::endpoints::exemem_api_url_for(
                folddb_profile::endpoints::Environment::Dev,
            )
            .to_string(),
            (None, Some("prod")) => folddb_profile::endpoints::exemem_api_url_for(
                folddb_profile::endpoints::Environment::Prod,
            )
            .to_string(),
            (None, Some(other)) => {
                return Err(format!("unknown --env '{other}' (expected dev or prod)"))
            }
            (None, None) => folddb_profile::endpoints::exemem_api_url(),
        };
        return runtime.block_on(cloud::connect(&home, &url, invite_code.as_deref(), force));
    }

    // Schema resolve/register must go to the same environment the home syncs
    // to (papercut-lastdbd-connect-env-dev-still-uses-prod-schema-service).
    cloud::pin_schema_service_environment_from_home(&home);

    // --- Uptime + crash attribution (best-effort; never blocks serving) ------
    // A Rust panic ⇒ a durable local crash report. Under `panic = "abort"` the
    // hook is the last code to run, so the report lands before the process dies.
    lastdb_node::crash_attribution::install_crash_hook(&home);
    // Record this session's start (reading the prior session for clean/unclean +
    // downtime + OS shutdown cause), then heartbeat every ~60s so the NEXT boot
    // can compute downtime even if this session dies uncleanly.
    let pid = std::process::id();
    let session_ledger =
        match lastdb_node::session_ledger::Ledger::record_start(&home, pid, 0, None) {
            Ok((ledger, summary)) => {
                if let Some(line) = summary.log_line() {
                    if summary.prev_session_clean {
                        tracing::info!(target: "lastdbd::session_ledger", "{line}");
                    } else {
                        tracing::warn!(target: "lastdbd::session_ledger", "{line}");
                    }
                }
                // Promote previous-session crash evidence to Sentry: surfaced
                // panic reports + a single unclean_exit event (carrying the
                // previous daemon log tail) when the prior session died with no
                // panic report to explain it. No-op without a bound client.
                let reports = lastdb_node::crash_attribution::scan_previous_crashes(&home);
                let tail = lastdb_node::crash_attribution::previous_log_tail();
                lastdb_node::crash_attribution::promote_previous_crash_evidence(
                    &reports,
                    Some(&summary),
                    tail.as_deref(),
                );
                Some(ledger)
            }
            Err(e) => {
                tracing::warn!(
                    target: "lastdbd::session_ledger",
                    error = %e,
                    "couldn't record session start; uptime ledger disabled for this run"
                );
                // Still surface panic reports even if the ledger write failed.
                let reports = lastdb_node::crash_attribution::scan_previous_crashes(&home);
                let tail = lastdb_node::crash_attribution::previous_log_tail();
                lastdb_node::crash_attribution::promote_previous_crash_evidence(
                    &reports,
                    None,
                    tail.as_deref(),
                );
                None
            }
        };
    let session_heartbeat = session_ledger.as_ref().and_then(|ledger| {
        ledger
            .spawn_heartbeat(std::time::Duration::from_secs(60))
            .ok()
    });

    // Every fatal error from here on exits the process through `main`'s `Err`
    // return. That is a CONTROLLED exit, but it leaves the session ledger line
    // open, which the next boot reads as "no clean shutdown record" ⇒ a phantom
    // native crash. Stamp the real reason on the way out so the next boot
    // reports the failure below instead of guessing at a crash it never saw.
    let note_fatal = |err: String| -> String {
        if session_ledger.is_some() {
            if let Err(e) = lastdb_node::session_ledger::mark_startup_failed(&home, pid, Some(&err))
            {
                tracing::warn!(
                    target: "lastdbd::session_ledger",
                    error = %e,
                    "couldn't record the startup failure in the session ledger"
                );
            }
        }
        err
    };

    let host = Arc::new(runtime.block_on(Host::boot(&home)).map_err(&note_fatal)?);
    // Capture this process's own boot identity once, from the ledger entry
    // this same process just wrote — not from a re-read of the shared
    // per-home ledger file, which a different process sharing this home can
    // append a newer row to after this point
    // (papercut-lastdb-primary-boot-identity-stale-phantom-pid-20260927).
    if let Some(ledger) = &session_ledger {
        let _ = host.own_boot_identity.set(ledger.own_record());
    }
    let shutdown = Arc::new(AtomicBool::new(false));
    let sample_interval = lastdb_node::self_metrics::sample_interval_from_env();
    let (sampler_task, home_fold_task) = {
        let _runtime_guard = runtime.enter();
        (
            lastdb_node::self_metrics::spawn_sampler(
                Arc::clone(&host),
                sample_interval,
                Arc::clone(&shutdown),
            ),
            spawn_home_conflict_fold(Arc::clone(&host), Arc::clone(&shutdown)),
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

    // Graceful shutdown: SIGTERM/SIGINT set the flag; the accept loop exits
    // on its next poll and the socket file is unlinked on drop.
    for signal in [libc::SIGTERM, libc::SIGINT] {
        let flag = Arc::clone(&shutdown);
        // SAFETY: the handler only performs an atomic store, which is
        // async-signal-safe.
        unsafe {
            signal_hook_registry::register(signal, move || {
                flag.store(true, Ordering::Relaxed);
            })
        }
        .map_err(|e| note_fatal(format!("failed to register signal handler: {e}")))?;
    }

    let owner_uid = unsafe { libc::getuid() };
    let handle = runtime.handle().clone();

    // Primary concurrency control: bounded worker pool + queue (CPU-shaped).
    // Accept no longer spawns unbounded OS threads (EMFILE incident 2026-07-15).
    // When the queue is full, peers get immediate 503 backpressure
    // (`uds_worker_queue_full`) instead of a "max connections" story.
    // Handler work is still budgeted via exec::block_on_route
    // (LASTDB_UDS_HANDLER_TIMEOUT_SECS). Env: LASTDB_UDS_WORKERS, LASTDB_UDS_QUEUE.
    let worker_pool = UdsWorkerPool::from_env();
    host.attach_uds_workers(worker_pool.clone());
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

    // Full-surface setup socket: same data surface PLUS the setup verbs
    // first-time clients need (`POST /api/schemas/load` — fkanban init /
    // fbrain bootstrap probe `<data>/folddb-full.sock` for schema loads, as
    // they do against the full node). Same owner-only peer-cred gate as the
    // narrow socket.
    let full_socket =
        UdsSocket::bind_at(full_socket_path.unwrap_or_else(|| data_dir.join("folddb-full.sock")))
            .map_err(|e| note_fatal(format!("failed to bind full-surface socket: {e}")))?;
    let full_accept = {
        let host = Arc::clone(&host);
        let handle = handle.clone();
        let shutdown = Arc::clone(&shutdown);
        let worker_pool = worker_pool.clone();
        std::thread::spawn(move || {
            let outcome = full_socket.serve(
                owner_uid,
                &shutdown,
                |_handle| CallerVerification::Unverified,
                |stream, transport, verification| {
                    let host = Arc::clone(&host);
                    let handle = handle.clone();
                    let owner_user_id = host.user_hash.clone();
                    if let Err(err) = worker_pool.try_submit_connection(stream, move |mut stream| {
                        let outcome = uds_http::serve_connection(
                            &mut stream,
                            &owner_user_id,
                            SocketKind::Owner,
                            transport,
                            verification,
                            |req, ctx| {
                                // Setup verbs first; everything else shares the
                                // narrow socket's dispatch table.
                                let path = req.target.split(['?', '#']).next();
                                if req.method == "POST" && path == Some("/api/schemas/load") {
                                    return exec::block_on_route(
                                        &handle,
                                        ctx,
                                        exec::execute_load_schemas_route(req, ctx, &host),
                                    );
                                }
                                if req.method == "POST" && path == Some("/api/apps/declare-schema")
                                {
                                    // Same Schema Service PoW + register wall
                                    // clock as /api/schemas/declare — use the
                                    // admin budget class (see block_on_data_route).
                                    return exec::block_on_admin_route(
                                        &handle,
                                        ctx,
                                        exec::execute_apps_declare_schema_route(req, ctx, &host),
                                    );
                                }
                                if req.method == "POST"
                                    && path == Some("/api/apps/verify-distribution-ready")
                                {
                                    return exec::block_on_route(
                                        &handle,
                                        ctx,
                                        exec::execute_apps_verify_distribution_ready_route(
                                            req, ctx, &host,
                                        ),
                                    );
                                }
                                if req.method == "POST"
                                    && path == Some("/api/apps/shared-surface/publish-attach")
                                {
                                    return exec::block_on_route(
                                        &handle,
                                        ctx,
                                        exec::execute_apps_shared_surface_publish_attach_route(
                                            req, ctx, &host,
                                        ),
                                    );
                                }
                                if req.method == "GET"
                                    && path == Some("/api/apps/shared-surface/attachments")
                                {
                                    return exec::block_on_route(
                                        &handle,
                                        ctx,
                                        exec::execute_apps_shared_surface_attachments_route(
                                            req, ctx, &host,
                                        ),
                                    );
                                }
                                uds_router::dispatch(
                                    req,
                                    ctx,
                                    SocketKind::Owner,
                                    |route, request, context| {
                                        exec::block_on_data_route(
                                            &handle, route, request, context, &host,
                                        )
                                    },
                                    uds_router::no_pairing_mint,
                                )
                            },
                        );
                        // Same thread that ran the request. Do not collect
                        // another worker. Under the slack line this is a no-op.
                        collect_finished_request_heap();
                        if let Err(e) = outcome {
                            tracing::debug!(error = %e, "full-socket connection ended with error");
                        }
                    }) {
                        // 503 already written inside the pool on QueueFull/ShutDown.
                        match err {
                            SubmitError::QueueFull => {
                                tracing::warn!(
                                    workers = worker_pool.workers(),
                                    queue_capacity = worker_pool.queue_capacity(),
                                    in_flight = worker_pool.in_flight(),
                                    rejects = worker_pool.queue_full_rejects(),
                                    socket = "full",
                                    "uds worker queue full; rejecting peer with 503"
                                );
                            }
                            SubmitError::ShutDown => {
                                tracing::error!(
                                    socket = "full",
                                    "uds worker pool shut down; rejecting peer"
                                );
                            }
                        }
                    }
                },
            );
            outcome.map_err(|error| format!("full-surface accept loop failed: {error}"))
        })
    };

    let accept_result = socket.serve(
        owner_uid,
        &shutdown,
        // No macOS code-signature verifier in the minimal daemon: every
        // same-user peer keeps the base `Unverified` posture, which the
        // owner socket maps to owner context — the device-trust model the
        // full node applies on this socket today.
        |_handle| CallerVerification::Unverified,
        |stream, transport, verification| {
            let host = Arc::clone(&host);
            let handle = handle.clone();
            let owner_user_id = host.user_hash.clone();
            if let Err(err) = worker_pool.try_submit_connection(stream, move |mut stream| {
                let outcome = uds_http::serve_connection(
                    &mut stream,
                    &owner_user_id,
                    SocketKind::Owner,
                    transport,
                    verification,
                    |req, ctx| {
                        // Local app-schema declaration is also on the owner
                        // data socket so collapsed Mini nodes (no separate
                        // full sock routing needed for this verb) can still
                        // run `brain init` without the schema_service path.
                        let path = req.target.split(['?', '#']).next();
                        if req.method == "POST" && path == Some("/api/apps/declare-schema") {
                            // Schema mutation PoW may exceed the 90s default
                            // handler budget; use admin class (600s default).
                            return exec::block_on_admin_route(
                                &handle,
                                ctx,
                                exec::execute_apps_declare_schema_route(req, ctx, &host),
                            );
                        }
                        if req.method == "POST"
                            && path == Some("/api/apps/verify-distribution-ready")
                        {
                            return exec::block_on_route(
                                &handle,
                                ctx,
                                exec::execute_apps_verify_distribution_ready_route(req, ctx, &host),
                            );
                        }
                        if req.method == "POST"
                            && path == Some("/api/apps/shared-surface/publish-attach")
                        {
                            return exec::block_on_route(
                                &handle,
                                ctx,
                                exec::execute_apps_shared_surface_publish_attach_route(
                                    req, ctx, &host,
                                ),
                            );
                        }
                        if req.method == "GET"
                            && path == Some("/api/apps/shared-surface/attachments")
                        {
                            return exec::block_on_route(
                                &handle,
                                ctx,
                                exec::execute_apps_shared_surface_attachments_route(
                                    req, ctx, &host,
                                ),
                            );
                        }
                        uds_router::dispatch(
                            req,
                            ctx,
                            SocketKind::Owner,
                            |route, request, context| {
                                exec::block_on_data_route(&handle, route, request, context, &host)
                            },
                            // No browser-pairing surface in the minimal
                            // daemon — the mint verb answers 404.
                            uds_router::no_pairing_mint,
                        )
                    },
                );
                collect_finished_request_heap();
                if let Err(e) = outcome {
                    tracing::debug!(error = %e, "control-socket connection ended with error");
                }
            }) {
                match err {
                    SubmitError::QueueFull => {
                        tracing::warn!(
                            workers = worker_pool.workers(),
                            queue_capacity = worker_pool.queue_capacity(),
                            in_flight = worker_pool.in_flight(),
                            rejects = worker_pool.queue_full_rejects(),
                            socket = "owner",
                            "uds worker queue full; rejecting peer with 503"
                        );
                    }
                    SubmitError::ShutDown => {
                        tracing::error!(
                            socket = "owner",
                            "uds worker pool shut down; rejecting peer"
                        );
                    }
                }
            }
        },
    );
    shutdown.store(true, Ordering::Release);
    let mut shutdown_errors = Vec::new();
    if let Err(error) = accept_result {
        shutdown_errors.push(format!("owner accept loop failed: {error}"));
    }

    // The accept loop returned — a graceful SIGTERM/SIGINT shutdown.
    // Persist that supervised intent BEFORE the bounded async drain. launchd's
    // kill window can expire while shutdown is still flushing; without this
    // intermediate disposition the next boot misclassifies every such restart
    // as a native crash and emits a false `unclean_exit` Sentry event.
    if session_ledger.is_some() {
        if let Err(e) = lastdb_node::session_ledger::mark_shutdown_started_with_reason(
            &home,
            pid,
            Some("signal (graceful shutdown started)"),
        ) {
            tracing::warn!(
                target: "lastdbd::session_ledger",
                error = %e,
                "couldn't record graceful shutdown intent in the session ledger"
            );
            shutdown_errors.push(format!("could not record shutdown intent: {e}"));
        }
    } else {
        shutdown_errors.push("session ledger is absent".to_string());
    }

    match full_accept.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => shutdown_errors.push(error),
        Err(_) => shutdown_errors.push("full-surface accept thread panicked".to_string()),
    }
    if let Err(error) = worker_pool.close_and_drain(shutdown_handler_drain_timeout()) {
        shutdown_errors.push(format!("request handlers did not drain: {error}"));
    }
    let background_result = runtime.block_on(async {
        let drain = async {
            sampler_task
                .await
                .map_err(|error| format!("self-metrics task failed: {error}"))?;
            home_fold_task
                .await
                .map_err(|error| format!("home conflict task failed: {error}"))?;
            while host.background_writes_in_flight() {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Ok::<(), String>(())
        };
        tokio::time::timeout(std::time::Duration::from_secs(180), drain)
            .await
            .map_err(|_| "background writers did not drain within 180 seconds".to_string())?
    });
    if let Err(error) = background_result {
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
    let shutdown_result = complete_shutdown_proof(&home, pid, start_ts, &shutdown_errors);
    shutdown_runtime::finish(runtime, shutdown_result)?;

    println!("lastdbd: shutdown signal received, exiting");
    Ok(())
}
