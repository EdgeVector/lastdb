//! `lastdbd` — the LastDB Mini semantic daemon and owner-socket server.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use lastdb_node::host::{self, Host};
use lastdb_node::lastdb_uds::uds_http::{self, UdsRequest, UdsResponse};
use lastdb_node::lastdb_uds::uds_router::{self, ControlRoute, DataRoute};
use lastdb_node::lastdb_uds::{
    self, ConnectionWork, SubmitError, UdsSocket, UdsWorkerPool, DATABASES_PATH,
    DATABASE_SCOPE_PREFIX,
};
use lastdb_node::{cloud, exec, jobs, log_sync};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tracing::Instrument;

const MAX_PARSE_BYTES: usize =
    uds_http::MAX_BODY_LEN + 4 * uds_http::MAX_HEADERS_TOTAL + 4 * uds_http::MAX_LINE_LEN;
const PARSE_PERMITS: u32 = MAX_PARSE_BYTES as u32;
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

#[derive(Clone)]
struct Server {
    host: Arc<Host>,
    handle: tokio::runtime::Handle,
    pool: UdsWorkerPool,
    body_budget: Arc<Semaphore>,
    mutation_replies: Arc<Semaphore>,
}

fn log_io(res: &std::io::Result<()>) {
    if let Err(e) = res {
        tracing::debug!(error = %e, "socket connection ended with error");
    }
}

fn retain_permits(lease: &mut OwnedSemaphorePermit, req: &UdsRequest) -> bool {
    let hdr_vec = req.headers.capacity() * std::mem::size_of::<(String, String)>();
    let base = std::mem::size_of::<UdsRequest>()
        + req.body.capacity()
        + req.method.capacity()
        + req.target.capacity()
        + hdr_vec;
    let bytes = req
        .headers
        .iter()
        .fold(base, |t, (k, v)| t + k.capacity() + v.capacity());
    if bytes > lease.num_permits() {
        return false;
    }
    drop(lease.split(lease.num_permits() - bytes));
    true
}

fn serve_conn(srv: &Server, stream: UnixStream, comp: ConnectionWork) -> std::io::Result<()> {
    if let Ok(lease) = Arc::clone(&srv.body_budget).try_acquire_many_owned(PARSE_PERMITS) {
        return serve_reserved(srv, stream, comp, lease);
    }
    let srv = srv.clone();
    srv.handle.clone().spawn(async move {
        match Arc::clone(&srv.body_budget)
            .acquire_many_owned(PARSE_PERMITS)
            .await
        {
            Ok(lease) => comp.resume(stream, move |s, c| {
                log_io(&serve_reserved(&srv, s, c, lease));
            }),
            Err(e) => tracing::debug!(error = %e, "socket connection ended with error"),
        }
    });
    Ok(())
}

fn serve_reserved(
    srv: &Server,
    mut stream: UnixStream,
    comp: ConnectionWork,
    mut lease: OwnedSemaphorePermit,
) -> std::io::Result<()> {
    let Some((mut req, ctx)) = uds_http::receive_connection(&mut stream, &srv.host.user_hash)?
    else {
        return Ok(());
    };
    if !exec::admit_database_session(&req, &srv.host) {
        let response = UdsResponse::new(403, "Forbidden", b"database session refused".to_vec());
        return uds_http::finish_connection(&mut stream, &response);
    }
    let deferred = match uds_router::route(&req) {
        ControlRoute::Data(r @ (DataRoute::Mutation | DataRoute::MutationBatch)) => Some(Some(r)),
        _ if exec::is_deferred_database_request(&req) => Some(None),
        _ => None,
    };
    if let Some(route) = deferred.filter(|_| retain_permits(&mut lease, &req)) {
        let (host, replies) = (Arc::clone(&srv.host), Arc::clone(&srv.mutation_replies));
        srv.handle.spawn(async move {
            let Ok(slot) = replies.acquire_owned().await else {
                return;
            };
            let resp = if let Some(r) = route {
                exec::execute_deferred_mutation(r, &req, &ctx, &host).await
            } else {
                exec::execute_deferred_database_request(&mut req, &ctx, &host).await
            };
            drop((req, lease));
            comp.resume(stream, move |mut s, _| {
                let res = uds_http::finish_connection(&mut s, &resp);
                drop((resp, slot));
                log_io(&res);
            });
        });
        return Ok(());
    }
    let path = req.target.split(['?', '#']).next();
    let resp = if req.target == DATABASES_PATH || req.target.starts_with(DATABASE_SCOPE_PREFIX) {
        exec::block_on_route(
            &srv.handle,
            &ctx,
            false,
            exec::execute_database_request(&mut req, &ctx, &srv.host),
        )
    } else if req.method == "GET" && path == Some("/health") {
        if srv.host.db.db_ops().native_runtime().requires_recovery() {
            let body = br#"{"status":"error","error":"storage_requires_recovery"}"#.to_vec();
            UdsResponse::new(503, "Service Unavailable", body)
                .with_header("Content-Type", "application/json")
        } else {
            uds_router::health_response()
        }
    } else if req.method == "POST" && path == Some("/api/apps/declare-schema") {
        exec::block_on_route(
            &srv.handle,
            &ctx,
            true,
            exec::execute_apps_declare_schema_route(&req, &ctx, &srv.host),
        )
    } else {
        uds_router::dispatch(&req, &ctx, |r, rq, cx| {
            exec::block_on_data_route(&srv.handle, r, rq, cx, &srv.host)
        })
    };
    uds_http::finish_connection(&mut stream, &resp)
}

fn spawn_background(
    rt: &tokio::runtime::Runtime,
    host: &Arc<Host>,
    stop: &Arc<AtomicBool>,
    reopen: Arc<AtomicBool>,
    sock: &Path,
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
        (
            "saved queries",
            jobs::saved_query::spawn(Arc::clone(host), sock.to_path_buf(), Arc::clone(stop)),
        ),
    ]
}

fn finish_shutdown(
    rt: tokio::runtime::Runtime,
    (home, pid): (&Path, u32),
    host: &Arc<Host>,
    pool: &UdsWorkerPool,
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
        bg_err,
        recv_err,
        own_err,
        db_err,
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
    rt.shutdown_timeout(Duration::from_secs(1));
    res?;
    println!("lastdbd: shutdown signal received, exiting");
    Ok(())
}

fn serve_node(
    rt: tokio::runtime::Runtime,
    home: &Path,
    sock_path: Option<PathBuf>,
) -> Result<(), String> {
    use fold_db::content_limit::{self as cl, MAX_FIELD_VALUE_BYTES_ENV};
    let (data_dir, pid) = (home.join("data"), std::process::id());
    rt.block_on(log_sync::install_prepared_native_recovery(home))?;
    let host = Arc::new(rt.block_on(Host::boot(home))?);
    let reopen = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let target_sock = sock_path.unwrap_or_else(|| data_dir.join(lastdb_uds::SOCKET_FILE_NAME));
    let socket = UdsSocket::bind_at(target_sock)
        .map_err(|e| format!("failed to bind socket in {}: {e}", data_dir.display()))?;
    println!("lastdbd serving the owner socket");
    let user_stop = Arc::new(AtomicBool::new(false));
    for sig in [libc::SIGTERM, libc::SIGINT] {
        let f = Arc::clone(&stop);
        let user_stop = Arc::clone(&user_stop);
        // SAFETY: the handler only performs an async-signal-safe atomic store.
        unsafe {
            signal_hook_registry::register(sig, move || {
                user_stop.store(true, Ordering::Relaxed);
                f.store(true, Ordering::Relaxed);
            })
        }
        .map_err(|e| format!("failed to register signal handler: {e}"))?;
    }
    let pool = UdsWorkerPool::from_env();
    let _ = host.uds_workers.set(pool.clone());
    let s = pool.snapshot();
    tracing::info!(
        workers = s.workers,
        queue_capacity = s.queue_capacity,
        "uds worker pool enabled"
    );
    cl::set_max_field_value_bytes(cl::parse_max_field_value_bytes(
        std::env::var(MAX_FIELD_VALUE_BYTES_ENV).ok(),
    ));
    tracing::info!(
        max_field_value_bytes = cl::max_field_value_bytes(),
        default = cl::DEFAULT_MAX_FIELD_VALUE_BYTES,
        absolute_max = cl::ABSOLUTE_MAX_FIELD_VALUE_BYTES,
        "peer field value size limit"
    );
    let permits = s
        .workers
        .saturating_mul(MAX_PARSE_BYTES)
        .min(Semaphore::MAX_PERMITS);
    let srv = Server {
        host: Arc::clone(&host),
        handle: rt.handle().clone(),
        pool: pool.clone(),
        body_budget: Arc::new(Semaphore::new(permits)),
        mutation_replies: Arc::new(Semaphore::new(s.workers)),
    };
    let tasks = spawn_background(&rt, &host, &stop, Arc::clone(&reopen), socket.path());
    // SAFETY: `getuid()` has no preconditions.
    let uid = unsafe { libc::getuid() };
    let accept_err = socket
        .serve(uid, &stop, |stream| {
            let c = srv.clone();
            match srv
                .pool
                .try_submit_owned_connection(stream, move |st, w| log_io(&serve_conn(&c, st, w)))
            {
                Ok(()) => {}
                Err(SubmitError::QueueFull) => {
                    let p = srv.pool.snapshot();
                    tracing::warn!(
                        workers = p.workers,
                        queue_capacity = p.queue_capacity,
                        in_flight = p.in_flight,
                        rejects = p.queue_full_rejects,
                        "uds worker queue full"
                    );
                }
                Err(SubmitError::ShutDown) => tracing::error!("uds worker pool shut down"),
            }
        })
        .map_err(|e| format!("owner accept loop failed: {e}"));
    stop.store(true, Ordering::Release);
    finish_shutdown(rt, (home, pid), &host, &pool, tasks, accept_err)?;
    drop(srv);
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
