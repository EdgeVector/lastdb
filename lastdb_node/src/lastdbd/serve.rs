//! Owner socket serving: request routing, body-byte budget, deferred
//! mutations, the accept handler, and worker-pool backpressure logging.

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

// Include body, bounded headers, request line and container allocation slack.
const MAX_PARSE_BYTES: usize =
    uds_http::MAX_BODY_LEN + 4 * uds_http::MAX_HEADERS_TOTAL + 4 * uds_http::MAX_LINE_LEN;

/// Deferred requests share the existing pool's maximum parse-byte allowance.
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

    fn try_reserve(&self) -> Option<BodyLease> {
        let bytes = u32::try_from(MAX_PARSE_BYTES).expect("bounded HTTP parse size fits u32");
        Arc::clone(&self.0)
            .try_acquire_many_owned(bytes)
            .ok()
            .map(BodyLease)
    }

    async fn reserve(&self) -> std::io::Result<BodyLease> {
        let bytes = u32::try_from(MAX_PARSE_BYTES).expect("bounded HTTP parse size fits u32");
        Arc::clone(&self.0)
            .acquire_many_owned(bytes)
            .await
            .map(BodyLease)
            .map_err(std::io::Error::other)
    }
}

impl BodyLease {
    /// Retain the actual frame bytes while its future awaits. Large frames
    /// consume the same allowance as the original bounded synchronous parsers.
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
        let unused = self.0.num_permits() - bytes;
        drop(self.0.split(unused));
        true
    }
}

/// Shared state every accept handler needs.
#[derive(Clone)]
pub(crate) struct Server {
    pub(crate) host: Arc<Host>,
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) worker_pool: UdsWorkerPool,
    body_budget: BodyBudget,
    mutation_replies: Arc<Semaphore>,
}

/// App verbs served on the owner socket. `None` means "not one of these".
fn route_app_verb(
    handle: &tokio::runtime::Handle,
    host: &Arc<Host>,
    req: &UdsRequest,
    ctx: &AccessContext,
    path: Option<&str>,
) -> Option<UdsResponse> {
    match (req.method.as_str(), path) {
        // Schema mutation PoW may exceed the 90s default handler budget, and
        // it shares the Schema Service wall clock with /api/schemas/declare:
        // use the admin budget class (600s default).
        ("POST", Some("/api/apps/declare-schema")) => Some(exec::block_on_admin_route(
            handle,
            ctx,
            exec::execute_apps_declare_schema_route(req, ctx, host),
        )),
        _ => None,
    }
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
        return storage_health_response(host);
    }
    if let Some(response) = route_app_verb(handle, host, req, ctx, path) {
        return response;
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

/// A reachable daemon cannot report success while storage refuses record calls.
/// This probe uses only the admission failure atomic, without database I/O or locks.
fn storage_health_response(host: &Host) -> UdsResponse {
    if host.db.db_ops().native_runtime().requires_recovery() {
        return UdsResponse::new(
            503,
            "Service Unavailable",
            br#"{"status":"error","error":"storage_requires_recovery"}"#.to_vec(),
        )
        .with_header("Content-Type", "application/json");
    }
    uds_router::health_response()
}

/// The pool already wrote the 503 on `QueueFull` and `ShutDown`; log why.
fn log_submit_error(pool: &UdsWorkerPool, err: SubmitError) {
    match err {
        SubmitError::QueueFull => {
            tracing::warn!(
                workers = pool.workers(),
                queue_capacity = pool.queue_capacity(),
                in_flight = pool.in_flight(),
                rejects = pool.queue_full_rejects(),
                "uds worker queue full; rejecting peer with 503"
            );
        }
        SubmitError::ShutDown => {
            tracing::error!("uds worker pool shut down; rejecting peer");
        }
    }
}

fn serve_connection(
    server: &Server,
    stream: UnixStream,
    completion: ConnectionWork,
) -> std::io::Result<()> {
    // Workers never wait for byte permits: queued responses must stay runnable.
    if let Some(body) = server.body_budget.try_reserve() {
        return serve_reserved(server, stream, completion, body);
    }
    let server = server.clone();
    server.handle.clone().spawn(async move {
        match server.body_budget.reserve().await {
            Ok(body) => completion.resume(stream, move |stream, completion| {
                let result = serve_reserved(&server, stream, completion, body);
                log_result(result);
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
            defer(
                server,
                Pending {
                    stream,
                    request,
                    context,
                    body,
                    completion,
                },
                route,
            );
            return Ok(());
        }
    }
    let response = route_request(&server.handle, &server.host, &mut request, &context);
    uds_http::finish_connection(&mut stream, &response)
}

struct Pending {
    stream: UnixStream,
    request: UdsRequest,
    context: AccessContext,
    body: BodyLease,
    completion: ConnectionWork,
}

enum MutationRoute {
    Default(uds_router::DataRoute),
    Scoped,
}

fn defer(server: &Server, pending: Pending, route: MutationRoute) {
    let host = Arc::clone(&server.host);
    let replies = Arc::clone(&server.mutation_replies);
    server.handle.spawn(async move {
        // Hold one slot from handler start through final write/drop. Neither
        // handler outputs nor slow-peer replies can multiply by queue capacity.
        let Ok(reply_slot) = replies.acquire_owned().await else {
            return;
        };
        let Pending {
            stream,
            mut request,
            context,
            body,
            completion,
        } = pending;
        let response = match route {
            MutationRoute::Default(route) => {
                exec::execute_deferred_mutation(route, &request, &context, &host).await
            }
            MutationRoute::Scoped => {
                exec::execute_deferred_database_mutation(&mut request, &context, &host).await
            }
        };
        // Release frame bytes before scheduling the response. Parsers that wait
        // for byte capacity must not prevent these responses from reaching a worker.
        drop(request);
        drop(body);
        completion.resume(stream, move |mut stream, _completion| {
            let result = uds_http::finish_connection(&mut stream, &response);
            drop(response);
            drop(reply_slot);
            log_result(result);
        });
    });
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

    fn on_accept(&self, stream: UnixStream) {
        let server = self.clone();
        let submitted =
            self.worker_pool
                .try_submit_owned_connection(stream, move |stream, completion| {
                    let result = serve_connection(&server, stream, completion);
                    log_result(result);
                });
        if let Err(err) = submitted {
            log_submit_error(&self.worker_pool, err);
        }
    }

    /// Run the owner socket accept loop until `shutdown` is set.
    pub(crate) fn serve(
        &self,
        socket: &UdsSocket,
        owner_uid: u32,
        shutdown: &AtomicBool,
    ) -> std::io::Result<()> {
        socket.serve(owner_uid, shutdown, |stream| {
            self.on_accept(stream);
        })
    }
}
