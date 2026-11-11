//! Owner socket serving: request routing, body-byte budget, and deferred mutations.

use std::os::unix::net::UnixStream;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use fold_db::access::AccessContext;
use lastdb_node::exec;
use lastdb_node::host::Host;
use lastdb_node::lastdb_uds::uds::UdsSocket;
use lastdb_node::lastdb_uds::uds_http::{self, UdsRequest, UdsResponse};
use lastdb_node::lastdb_uds::uds_router;
use lastdb_node::lastdb_uds::worker_pool::{ConnectionWork, SubmitError, UdsWorkerPool};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_PARSE_BYTES: usize =
    uds_http::MAX_BODY_LEN + 4 * uds_http::MAX_HEADERS_TOTAL + 4 * uds_http::MAX_LINE_LEN;

#[derive(Clone)]
struct BodyBudget(Arc<Semaphore>);

struct BodyLease(OwnedSemaphorePermit);

impl BodyBudget {
    fn new(workers: usize) -> Self {
        let bytes = workers
            .saturating_mul(MAX_PARSE_BYTES)
            .min(Semaphore::MAX_PERMITS);
        Self(Arc::new(Semaphore::new(bytes)))
    }

    fn parse_permits() -> u32 {
        u32::try_from(MAX_PARSE_BYTES).expect("bounded HTTP parse size fits u32")
    }

    fn try_reserve(&self) -> Option<BodyLease> {
        Arc::clone(&self.0)
            .try_acquire_many_owned(Self::parse_permits())
            .ok()
            .map(BodyLease)
    }

    async fn reserve(&self) -> std::io::Result<BodyLease> {
        Arc::clone(&self.0)
            .acquire_many_owned(Self::parse_permits())
            .await
            .map(BodyLease)
            .map_err(std::io::Error::other)
    }
}

impl BodyLease {
    fn retain_request(&mut self, request: &UdsRequest) -> bool {
        let bytes = request.headers.iter().fold(
            std::mem::size_of::<UdsRequest>()
                + request.body.capacity()
                + request.method.capacity()
                + request.target.capacity()
                + request.headers.capacity() * std::mem::size_of::<(String, String)>(),
            |total, (name, value)| {
                total
                    .saturating_add(name.capacity())
                    .saturating_add(value.capacity())
            },
        );
        if bytes > self.0.num_permits() {
            return false;
        }
        drop(self.0.split(self.0.num_permits() - bytes));
        true
    }
}

#[derive(Clone)]
pub(crate) struct Server {
    host: Arc<Host>,
    handle: tokio::runtime::Handle,
    worker_pool: UdsWorkerPool,
    body_budget: BodyBudget,
    mutation_replies: Arc<Semaphore>,
}

fn route_request(
    handle: &tokio::runtime::Handle,
    host: &Arc<Host>,
    req: &mut UdsRequest,
    ctx: &AccessContext,
) -> UdsResponse {
    if req.target == lastdb_node::lastdb_uds::database_path::DATABASES_PATH
        || req
            .target
            .starts_with(lastdb_node::lastdb_uds::database_path::DATABASE_SCOPE_PREFIX)
    {
        return exec::block_on_route(handle, ctx, exec::execute_database_request(req, ctx, host));
    }
    let path = req.target.split(['?', '#']).next();
    if req.method == "GET" && path == Some("/health") {
        if host.db.db_ops().native_runtime().requires_recovery() {
            return UdsResponse::new(
                503,
                "Service Unavailable",
                br#"{"status":"error","error":"storage_requires_recovery"}"#.to_vec(),
            )
            .with_header("Content-Type", "application/json");
        }
        return uds_router::health_response();
    }
    if req.method == "POST" && path == Some("/api/apps/declare-schema") {
        return exec::block_on_admin_route(
            handle,
            ctx,
            exec::execute_apps_declare_schema_route(req, ctx, host),
        );
    }
    uds_router::dispatch(
        req,
        ctx,
        |route, request, context| exec::block_on_data_route(handle, route, request, context, host),
        (
            lastdb_node::version::MIN_SUPPORTED_API_VERSION,
            lastdb_node::version::CURRENT_OUTBOX_FORMAT_VERSION,
            lastdb_node::version::MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
        ),
    )
}

enum MutationRoute {
    Default(uds_router::DataRoute),
    Scoped,
}

fn serve_connection(
    server: &Server,
    stream: UnixStream,
    completion: ConnectionWork,
) -> std::io::Result<()> {
    if let Some(body) = server.body_budget.try_reserve() {
        return serve_reserved(server, stream, completion, body);
    }
    let server = server.clone();
    server.handle.clone().spawn(async move {
        match server.body_budget.reserve().await {
            Ok(body) => completion.resume(stream, move |stream, completion| {
                log_result(serve_reserved(&server, stream, completion, body));
            }),
            Err(error) => log_result(Err(error)),
        }
    });
    Ok(())
}

fn serve_reserved(
    server: &Server,
    mut stream: UnixStream,
    completion: ConnectionWork,
    mut body: BodyLease,
) -> std::io::Result<()> {
    let Some((mut request, context)) =
        uds_http::receive_connection(&mut stream, &server.host.user_hash)?
    else {
        return Ok(());
    };
    let route = match uds_router::route(&request) {
        uds_router::ControlRoute::Data(
            route @ (uds_router::DataRoute::Mutation | uds_router::DataRoute::MutationBatch),
        ) => Some(MutationRoute::Default(route)),
        _ if exec::is_deferred_database_mutation(&request) => Some(MutationRoute::Scoped),
        _ => None,
    };
    if let Some(route) = route {
        if body.retain_request(&request) {
            let host = Arc::clone(&server.host);
            let replies = Arc::clone(&server.mutation_replies);
            server.handle.spawn(async move {
                let Ok(reply_slot) = replies.acquire_owned().await else {
                    return;
                };
                let response = match route {
                    MutationRoute::Default(route) => {
                        exec::execute_deferred_mutation(route, &request, &context, &host).await
                    }
                    MutationRoute::Scoped => {
                        exec::execute_deferred_database_mutation(&mut request, &context, &host)
                            .await
                    }
                };
                drop(request);
                drop(body);
                completion.resume(stream, move |mut stream, _completion| {
                    let result = uds_http::finish_connection(&mut stream, &response);
                    drop(response);
                    drop(reply_slot);
                    log_result(result);
                });
            });
            return Ok(());
        }
    }
    let response = route_request(&server.handle, &server.host, &mut request, &context);
    uds_http::finish_connection(&mut stream, &response)
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
        let body_budget = BodyBudget::new(worker_pool.workers());
        let mutation_replies = Arc::new(Semaphore::new(worker_pool.workers()));
        Self {
            host,
            handle,
            worker_pool,
            body_budget,
            mutation_replies,
        }
    }

    pub(crate) fn serve(
        &self,
        socket: &UdsSocket,
        owner_uid: u32,
        shutdown: &AtomicBool,
    ) -> std::io::Result<()> {
        socket.serve(owner_uid, shutdown, |stream| {
            let server = self.clone();
            let submitted =
                self.worker_pool
                    .try_submit_owned_connection(stream, move |stream, completion| {
                        log_result(serve_connection(&server, stream, completion));
                    });
            match submitted {
                Ok(()) => {}
                Err(SubmitError::QueueFull) => tracing::warn!(
                    workers = self.worker_pool.workers(),
                    queue_capacity = self.worker_pool.queue_capacity(),
                    in_flight = self.worker_pool.in_flight(),
                    rejects = self.worker_pool.queue_full_rejects(),
                    "uds worker queue full; rejecting peer with 503"
                ),
                Err(SubmitError::ShutDown) => {
                    tracing::error!("uds worker pool shut down; rejecting peer");
                }
            }
        })
    }
}
