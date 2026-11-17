//! `lastdbd` — the LastDB Mini semantic daemon and owner-socket server.

use std::io::Write;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use lastdb_node::exec::UdsServer;
use lastdb_node::host::{self, Host};
use lastdb_node::lastdb_uds::{self, UdsSocket, UdsWorkerPool};
use lastdb_node::{cloud, jobs, log_sync};
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinHandle;
use tracing::Instrument;

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

fn spawn_background(
    rt: &tokio::runtime::Runtime,
    host: &Arc<Host>,
    stop: &Arc<AtomicBool>,
    reopen: Arc<AtomicBool>,
) -> Vec<(&'static str, JoinHandle<()>)> {
    let _g = rt.enter();
    let (sh, ss) = (Arc::clone(host), Arc::clone(stop));
    let sync = tokio::spawn(
        async move {
            if log_sync::spawn(sh, ss, reopen).await.is_err() {
                tracing::warn!(target: "lastdbd::log_sync", "log sync task stopped");
            }
        }
        .in_current_span(),
    );
    vec![
        (
            "ttl sweep",
            jobs::ttl_sweep::spawn_periodic(Arc::clone(host), Arc::clone(stop)),
        ),
        ("log sync", sync),
    ]
}

fn finish_shutdown(
    rt: tokio::runtime::Runtime,
    home: &Path,
    host: &Arc<Host>,
    pool: &UdsWorkerPool,
    app_pool: &UdsWorkerPool,
    tasks: Vec<(&'static str, JoinHandle<()>)>,
    accept_err: Result<(), String>,
) -> Result<(), String> {
    let drain_bg = async {
        let mut errs = Vec::new();
        for (name, task) in tasks {
            if let Err(e) = task.await {
                errs.push(format!("{name} task failed: {e}"));
            }
        }
        errs.is_empty().then_some(()).ok_or_else(|| errs.join("; "))
    };
    let pool_err = pool
        .close_and_drain(Duration::from_secs(180))
        .err()
        .map(|e| format!("request handlers did not drain: {e}"));
    let app_pool_err = app_pool
        .close_and_drain(Duration::from_secs(180))
        .err()
        .map(|e| format!("app socket handlers did not drain: {e}"));
    let bg_err = match rt
        .block_on(async { tokio::time::timeout(Duration::from_secs(180), drain_bg).await })
    {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e),
        Err(_) => Some("background writers did not drain within 180 seconds".to_string()),
    };
    let recv_err = rt.block_on(host.shutdown_received_databases()).err();
    let own_err = rt
        .block_on(host.shutdown_owned_databases())
        .err()
        .map(|e| format!("owned database shutdown flush failed: {e}"));
    let db_err = rt
        .block_on(host.db.shutdown())
        .err()
        .map(|e| format!("FoldDB shutdown flush failed: {e}"));
    let errs: Vec<String> = [
        accept_err.err(),
        pool_err,
        app_pool_err,
        bg_err,
        recv_err,
        own_err,
        db_err,
    ]
    .into_iter()
    .flatten()
    .collect();
    let res = if errs.is_empty() {
        host::write_shutdown_flush_receipt(home, std::process::id(), host.process_start_ts)
            .map_err(|e| format!("could not write shutdown flush proof: {e}"))
    } else {
        Err(format!(
            "shutdown did not prove a complete flush: {}",
            errs.join("; ")
        ))
    };
    rt.shutdown_timeout(Duration::from_secs(1));
    res?;
    println!("lastdbd: shutdown signal received, exiting");
    Ok(())
}

fn spawn_stop_signals(
    rt: &tokio::runtime::Runtime,
    stop: &Arc<AtomicBool>,
    user_stop: &Arc<AtomicBool>,
) -> Result<JoinHandle<()>, String> {
    let _guard = rt.enter();
    let mut term = signal(SignalKind::terminate())
        .map_err(|e| format!("failed to register signal handler: {e}"))?;
    let mut intr = signal(SignalKind::interrupt())
        .map_err(|e| format!("failed to register signal handler: {e}"))?;
    let (stop, user_stop) = (Arc::clone(stop), Arc::clone(user_stop));
    Ok(tokio::spawn(
        async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = intr.recv() => {}
            }
            user_stop.store(true, Ordering::Relaxed);
            stop.store(true, Ordering::Release);
        }
        .in_current_span(),
    ))
}

fn serve_node(
    rt: tokio::runtime::Runtime,
    home: &Path,
    sock_path: Option<PathBuf>,
) -> Result<(), String> {
    let data_dir = home.join("data");
    rt.block_on(log_sync::install_prepared_native_recovery(home))?;
    let host = Arc::new(rt.block_on(Host::boot(home))?);
    let reopen = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let user_stop = Arc::new(AtomicBool::new(false));
    let target_sock = sock_path.unwrap_or_else(|| data_dir.join(lastdb_uds::SOCKET_FILE_NAME));
    let app_sock = target_sock.with_file_name(lastdb_uds::APP_SOCKET_FILE_NAME);
    let socket = UdsSocket::bind_at(target_sock)
        .map_err(|e| format!("failed to bind socket in {}: {e}", data_dir.display()))?;
    let app_socket = UdsSocket::bind_at(app_sock)
        .map_err(|e| format!("failed to bind app socket in {}: {e}", data_dir.display()))?;
    let signal_task = spawn_stop_signals(&rt, &stop, &user_stop)?;
    println!("lastdbd serving the owner socket");
    let pool = UdsWorkerPool::from_env();
    let _ = host.uds_workers.set(pool.clone());
    let s = pool.snapshot();
    tracing::info!(
        workers = s.workers,
        queue_capacity = s.queue_capacity,
        "uds worker pool enabled"
    );
    let server = UdsServer::new(Arc::clone(&host), rt.handle().clone(), pool.clone());
    let tasks = spawn_background(&rt, &host, &stop, Arc::clone(&reopen));
    let app_server = server.for_apps();
    let app_pool = app_server.worker_pool();
    let app_snap = app_pool.snapshot();
    tracing::info!(
        workers = app_snap.workers,
        queue_capacity = app_snap.queue_capacity,
        "app socket worker pool enabled"
    );
    let app_stop = Arc::clone(&stop);
    let app_thread = std::thread::Builder::new()
        .name("lastdbd-app-socket".into())
        .spawn(move || {
            if let Err(error) = app_server.serve(&app_socket, &app_stop) {
                tracing::error!(error = %error, "app socket accept loop failed");
            }
        })
        .map_err(|e| format!("failed to start the app socket: {e}"))?;
    let accept_err = server.serve(&socket, &stop);
    stop.store(true, Ordering::Release);
    let _ = app_thread.join();
    signal_task.abort();
    finish_shutdown(rt, home, &host, &pool, &app_pool, tasks, accept_err)?;
    drop(server);
    drop(host);
    drop(socket);
    if reopen.load(Ordering::Acquire) && !user_stop.load(Ordering::Relaxed) {
        tracing::info!(target: "lastdbd::log_sync", "reopening the daemon for verified native recovery");
        let exe = std::env::current_exe().map_err(|error| error.to_string())?;
        let error = std::process::Command::new(exe)
            .args(std::env::args_os().skip(1))
            .exec();
        return Err(format!("could not reopen after native recovery: {error}"));
    }
    Ok(())
}

fn run() -> Result<(), String> {
    let _sentry = host::init_daemon_logging();
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
        lastdb_uds::preflight_data_dir(&home.join("data"))
            .map_err(|e| format!("cannot serve from {}: {e}", home.display()))?;
    }
    let stack = fold_db::env_flag::var_or("RUST_MIN_STACK", 0usize).max(64 * 1024 * 1024);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(stack)
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))?;
    cloud::pin_schema_service_environment_from_home(&home);
    serve_node(rt, &home, cli.socket_path)
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
