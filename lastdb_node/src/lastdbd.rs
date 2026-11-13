//! Owner socket serving, body-byte budget, deferred mutations, and background tasks.

use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use lastdb_node::exec;
use lastdb_node::host::Host;
use lastdb_node::lastdb_uds::database_path::{DATABASES_PATH, DATABASE_SCOPE_PREFIX};
use lastdb_node::lastdb_uds::uds::UdsSocket;
use lastdb_node::lastdb_uds::uds_http::{self, UdsRequest, UdsResponse};
use lastdb_node::lastdb_uds::uds_router;
use lastdb_node::lastdb_uds::worker_pool::{ConnectionWork, SubmitError, UdsWorkerPool};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tracing::Instrument;

const MAX_PARSE_BYTES: usize =
    uds_http::MAX_BODY_LEN + 4 * uds_http::MAX_HEADERS_TOTAL + 4 * uds_http::MAX_LINE_LEN;
const PARSE_PERMITS: u32 = MAX_PARSE_BYTES as u32;

pub(crate) fn spawn_background(
    runtime: &tokio::runtime::Runtime,
    host: &Arc<Host>,
    shutdown: &Arc<AtomicBool>,
    socket: &Path,
) -> Vec<(&'static str, JoinHandle<()>)> {
    let _guard = runtime.enter();
    let (sh, ss) = (Arc::clone(host), Arc::clone(shutdown));
    let sync = tokio::spawn(
        async move {
            if lastdb_node::log_sync::spawn(sh, ss).await.is_err() {
                tracing::warn!(target: "lastdbd::log_sync", "log sync task stopped");
            }
        }
        .in_current_span(),
    );
    vec![
        (
            "ttl sweep",
            lastdb_node::jobs::ttl_sweep::spawn_periodic(Arc::clone(host), Arc::clone(shutdown)),
        ),
        ("log sync", sync),
        (
            "received shares",
            lastdb_node::registered_share::spawn(host.home.clone(), Arc::clone(shutdown)),
        ),
        (
            "saved queries",
            lastdb_node::jobs::saved_query::spawn(
                Arc::clone(host),
                socket.to_path_buf(),
                Arc::clone(shutdown),
            ),
        ),
    ]
}

pub(crate) fn drain_background(
    runtime: &tokio::runtime::Runtime,
    tasks: Vec<(&'static str, JoinHandle<()>)>,
) -> Result<(), String> {
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(180), async {
            let mut errs = Vec::new();
            for (name, task) in tasks {
                if let Err(e) = task.await {
                    errs.push(format!("{name} task failed: {e}"));
                }
            }
            errs.is_empty().then_some(()).ok_or_else(|| errs.join("; "))
        })
        .await
        .map_err(|_| "background writers did not drain within 180 seconds".to_string())?
    })
}

fn retain_request_permits(lease: &mut OwnedSemaphorePermit, req: &UdsRequest) -> bool {
    let base = std::mem::size_of::<UdsRequest>()
        + req.body.capacity()
        + req.method.capacity()
        + req.target.capacity()
        + req.headers.capacity() * std::mem::size_of::<(String, String)>();
    let bytes = req
        .headers
        .iter()
        .fold(base, |t, (k, v)| t + k.capacity() + v.capacity());
    (bytes <= lease.num_permits())
        .then(|| drop(lease.split(lease.num_permits() - bytes)))
        .is_some()
}

#[derive(Clone)]
pub(crate) struct Server {
    host: Arc<Host>,
    handle: tokio::runtime::Handle,
    worker_pool: UdsWorkerPool,
    body_budget: Arc<Semaphore>,
    mutation_replies: Arc<Semaphore>,
}

fn serve_connection(
    server: &Server,
    stream: UnixStream,
    completion: ConnectionWork,
) -> std::io::Result<()> {
    if let Ok(lease) = Arc::clone(&server.body_budget).try_acquire_many_owned(PARSE_PERMITS) {
        return serve_reserved(server, stream, completion, lease);
    }
    let server = server.clone();
    server.handle.clone().spawn(async move {
        match Arc::clone(&server.body_budget)
            .acquire_many_owned(PARSE_PERMITS)
            .await
        {
            Ok(lease) => completion.resume(stream, move |s, c| {
                log_result(serve_reserved(&server, s, c, lease));
            }),
            Err(e) => log_result(Err(std::io::Error::other(e))),
        }
    });
    Ok(())
}

fn serve_reserved(
    server: &Server,
    mut stream: UnixStream,
    completion: ConnectionWork,
    mut lease: OwnedSemaphorePermit,
) -> std::io::Result<()> {
    let Some((mut req, ctx)) = uds_http::receive_connection(&mut stream, &server.host.user_hash)?
    else {
        return Ok(());
    };
    let deferred = match uds_router::route(&req) {
        uds_router::ControlRoute::Data(
            r @ (uds_router::DataRoute::Mutation | uds_router::DataRoute::MutationBatch),
        ) => Some(Some(r)),
        _ if exec::is_deferred_database_mutation(&req) => Some(None),
        _ => None,
    };
    if let Some(route) = deferred.filter(|_| retain_request_permits(&mut lease, &req)) {
        let (host, replies) = (
            Arc::clone(&server.host),
            Arc::clone(&server.mutation_replies),
        );
        server.handle.spawn(async move {
            let Ok(slot) = replies.acquire_owned().await else {
                return;
            };
            let resp = if let Some(r) = route {
                exec::execute_deferred_mutation(r, &req, &ctx, &host).await
            } else {
                exec::execute_deferred_database_mutation(&mut req, &ctx, &host).await
            };
            drop((req, lease));
            completion.resume(stream, move |mut s, _| {
                let res = uds_http::finish_connection(&mut s, &resp);
                drop((resp, slot));
                log_result(res);
            });
        });
        return Ok(());
    }
    let path = req.target.split(['?', '#']).next();
    let resp = if req.target == DATABASES_PATH || req.target.starts_with(DATABASE_SCOPE_PREFIX) {
        exec::block_on_route(
            &server.handle,
            &ctx,
            false,
            exec::execute_database_request(&mut req, &ctx, &server.host),
        )
    } else if req.method == "GET" && path == Some("/health") {
        if server.host.db.db_ops().native_runtime().requires_recovery() {
            UdsResponse::new(
                503,
                "Service Unavailable",
                br#"{"status":"error","error":"storage_requires_recovery"}"#.to_vec(),
            )
            .with_header("Content-Type", "application/json")
        } else {
            uds_router::health_response()
        }
    } else if req.method == "POST" && path == Some("/api/apps/declare-schema") {
        exec::block_on_route(
            &server.handle,
            &ctx,
            true,
            exec::execute_apps_declare_schema_route(&req, &ctx, &server.host),
        )
    } else {
        uds_router::dispatch(&req, &ctx, |r, request, context| {
            exec::block_on_data_route(&server.handle, r, request, context, &server.host)
        })
    };
    uds_http::finish_connection(&mut stream, &resp)
}

fn log_result(result: std::io::Result<()>) {
    if let Err(error) = result {
        tracing::debug!(error = %error, "socket connection ended with error");
    }
}

impl Server {
    pub(crate) fn new(
        host: Arc<Host>,
        handle: tokio::runtime::Handle,
        worker_pool: UdsWorkerPool,
    ) -> Self {
        let w = worker_pool.snapshot().workers;
        let permits = w
            .saturating_mul(MAX_PARSE_BYTES)
            .min(Semaphore::MAX_PERMITS);
        Self {
            host,
            handle,
            worker_pool,
            body_budget: Arc::new(Semaphore::new(permits)),
            mutation_replies: Arc::new(Semaphore::new(w)),
        }
    }

    pub(crate) fn serve(
        &self,
        socket: &UdsSocket,
        owner_uid: u32,
        shutdown: &AtomicBool,
    ) -> std::io::Result<()> {
        socket.serve(owner_uid, shutdown, |stream| {
            let srv = self.clone();
            match self
                .worker_pool
                .try_submit_owned_connection(stream, move |s, c| {
                    log_result(serve_connection(&srv, s, c));
                }) {
                Ok(()) => {}
                Err(SubmitError::QueueFull) => {
                    let s = self.worker_pool.snapshot();
                    tracing::warn!(
                        workers = s.workers,
                        queue_capacity = s.queue_capacity,
                        in_flight = s.in_flight,
                        rejects = s.queue_full_rejects,
                        "uds worker queue full; rejecting peer with 503"
                    );
                }
                Err(SubmitError::ShutDown) => {
                    tracing::error!("uds worker pool shut down; rejecting peer");
                }
            }
        })
    }
}
