//! Owner-socket and app-socket request framing, body-budget admission, and worker dispatch.

use std::os::unix::net::UnixStream;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use fold_db::access::AccessContext;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Instrument;

use super::{app_api, block_on_data_route, block_on_route, calling_app, database_routes};
use crate::host::{ops, Host};
use crate::lastdb_uds::uds_http::{self, UdsRequest, UdsResponse};
use crate::lastdb_uds::uds_router::{self, ControlRoute, DataRoute};
use crate::lastdb_uds::{ConnectionWork, SubmitError, UdsSocket, UdsWorkerPool};

/// The app socket does not share the owner's workers. An app that opens
/// connections and sends nothing can fill only this many workers, so it
/// cannot starve the owner socket.
const APP_SOCKET_WORKERS: usize = 4;
/// Queued connections in front of [`APP_SOCKET_WORKERS`]. As deep as the owner
/// socket's default (`LASTDB_UDS_QUEUE`, 256). A queued connection costs one
/// file descriptor. Refusing at eight broke ordinary parallel reads.
const APP_SOCKET_QUEUE: usize = 256;
/// No file bytes cross the app API, so a larger body is refused.
const APP_MAX_BODY_LEN: usize = 8 * 1024 * 1024;
const APP_BODY_TOO_LARGE: &str = "request body is larger than 8 MiB";

const fn parse_budget(max_body: usize) -> usize {
    max_body + 4 * uds_http::MAX_HEADERS_TOTAL + 4 * uds_http::MAX_LINE_LEN
}

#[derive(Clone)]
pub struct UdsServer {
    host: Arc<Host>,
    handle: tokio::runtime::Handle,
    pool: UdsWorkerPool,
    body_budget: Arc<Semaphore>,
    mutation_replies: Arc<Semaphore>,
    parse_permits: u32,
    read_policy: uds_http::ReadPolicy,
    /// True for the server of the app socket.
    apps: bool,
}

impl UdsServer {
    #[must_use]
    pub fn new(host: Arc<Host>, handle: tokio::runtime::Handle, pool: UdsWorkerPool) -> Self {
        Self::with_limits(host, handle, pool, uds_http::MAX_BODY_LEN, false)
    }

    /// The app-socket server. It shares the host and the runtime handle only.
    /// Its workers and its body budget are its own, so load on `apps.sock`
    /// can delay only other requests on `apps.sock`.
    #[must_use]
    pub fn for_apps(&self) -> Self {
        let pool = UdsWorkerPool::fixed(APP_SOCKET_WORKERS, APP_SOCKET_QUEUE);
        Self::with_limits(
            Arc::clone(&self.host),
            self.handle.clone(),
            pool,
            APP_MAX_BODY_LEN,
            true,
        )
    }

    #[must_use]
    pub fn worker_pool(&self) -> UdsWorkerPool {
        self.pool.clone()
    }

    fn with_limits(
        host: Arc<Host>,
        handle: tokio::runtime::Handle,
        pool: UdsWorkerPool,
        max_body: usize,
        apps: bool,
    ) -> Self {
        let snapshot = pool.snapshot();
        let budget = parse_budget(max_body);
        let permits = snapshot
            .workers
            .saturating_mul(budget)
            .min(Semaphore::MAX_PERMITS);
        let read_policy = if apps {
            uds_http::ReadPolicy::app(max_body)
        } else {
            uds_http::ReadPolicy::owner(max_body)
        };
        Self {
            host,
            handle,
            pool,
            body_budget: Arc::new(Semaphore::new(permits)),
            mutation_replies: Arc::new(Semaphore::new(snapshot.workers)),
            parse_permits: budget as u32,
            read_policy,
            apps,
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub fn serve(&self, socket: &UdsSocket, stop: &AtomicBool) -> Result<(), String> {
        // SAFETY: `getuid()` has no preconditions.
        let uid = unsafe { libc::getuid() };
        socket
            .serve(uid, stop, |stream| {
                let server = self.clone();
                match self
                    .pool
                    .try_submit_owned_connection(stream, move |st, work| {
                        log_io(&server.serve_conn(st, work));
                    }) {
                    Ok(()) => {}
                    Err(SubmitError::QueueFull) => {
                        let p = self.pool.snapshot();
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
            .map_err(|e| format!("owner accept loop failed: {e}"))
    }

    fn serve_conn(&self, stream: UnixStream, comp: ConnectionWork) -> std::io::Result<()> {
        if let Ok(lease) = Arc::clone(&self.body_budget).try_acquire_many_owned(self.parse_permits)
        {
            return self.serve_reserved(stream, comp, lease);
        }
        let server = self.clone();
        self.handle.spawn(
            async move {
                match Arc::clone(&server.body_budget)
                    .acquire_many_owned(server.parse_permits)
                    .await
                {
                    Ok(lease) => comp.resume(stream, move |s, c| {
                        log_io(&server.serve_reserved(s, c, lease));
                    }),
                    Err(e) => tracing::debug!(error = %e, "socket connection ended with error"),
                }
            }
            .in_current_span(),
        );
        Ok(())
    }

    fn serve_reserved(
        &self,
        mut stream: UnixStream,
        comp: ConnectionWork,
        mut lease: OwnedSemaphorePermit,
    ) -> std::io::Result<()> {
        let started = Instant::now();
        let (mut req, ctx) = match uds_http::receive_connection(
            &mut stream,
            &self.host.user_hash,
            &self.read_policy,
        )? {
            uds_http::Received::Finished => return Ok(()),
            uds_http::Received::BodyTooLarge(req) => {
                let response = app_api::refuse(413, APP_BODY_TOO_LARGE);
                let call = ops::call_of(&req.target);
                return self.finish(&mut stream, &req, call, &response, started);
            }
            uds_http::Received::Ready(req, ctx) => (req, ctx),
        };
        let call = ops::call_of(&req.target);
        if self.apps {
            let response = calling_app::serve(&self.handle, &req, &ctx, &self.host);
            return self.finish(&mut stream, &req, call, &response, started);
        }
        let routed = uds_router::route(&req);
        let deferred = match &routed {
            ControlRoute::Data(DataRoute::V1Write) => Some(true),
            ControlRoute::Database if database_routes::is_deferred_database_request(&req) => {
                Some(false)
            }
            _ => None,
        };
        if let Some(write) = deferred.filter(|_| retain_permits(&mut lease, &req)) {
            self.spawn_deferred(Deferred {
                stream,
                comp,
                lease,
                req,
                ctx,
                call,
                write,
                started,
            });
            return Ok(());
        }
        let resp = match routed {
            ControlRoute::Health => uds_router::health_response(
                self.host.db.db_ops().native_runtime().requires_recovery(),
            ),
            ControlRoute::Version => uds_router::version_response(),
            ControlRoute::Database => block_on_route(
                &self.handle,
                &ctx,
                database_routes::execute_database_request(&mut req, &ctx, &self.host),
            ),
            ControlRoute::Data(r) => block_on_data_route(&self.handle, r, &req, &ctx, &self.host),
            ControlRoute::MethodNotAllowed { allow } => {
                uds_router::method_not_allowed_response(allow)
            }
            ControlRoute::NotFound => UdsResponse::new(404, "Not Found", b"Not Found".to_vec()),
        };
        self.finish(&mut stream, &req, call, &resp, started)
    }

    fn finish(
        &self,
        stream: &mut UnixStream,
        req: &UdsRequest,
        call: ops::CallLabel,
        response: &UdsResponse,
        started: Instant,
    ) -> std::io::Result<()> {
        let wrote = uds_http::finish_connection(stream, response);
        self.host.ops.record(&ops::Completed {
            apps: self.apps,
            headers: &req.headers,
            body: &req.body,
            call,
            status: response.status,
            elapsed: started.elapsed(),
        });
        wrote
    }

    fn spawn_deferred(&self, job: Deferred) {
        let (host, replies) = (Arc::clone(&self.host), Arc::clone(&self.mutation_replies));
        self.handle.spawn(
            async move {
                let Ok(slot) = replies.acquire_owned().await else {
                    return;
                };
                let Deferred {
                    stream,
                    comp,
                    lease,
                    mut req,
                    ctx,
                    call,
                    write,
                    started,
                } = job;
                let resp = if write {
                    super::execute_data_route_with_timeout(DataRoute::V1Write, &req, &ctx, &host)
                        .await
                } else {
                    database_routes::execute_deferred_database_request(&mut req, &ctx, &host).await
                };
                host.ops.record(&ops::Completed {
                    apps: false,
                    headers: &req.headers,
                    body: &req.body,
                    call,
                    status: resp.status,
                    elapsed: started.elapsed(),
                });
                drop((req, lease));
                comp.resume(stream, move |mut socket, _| {
                    let res = uds_http::finish_connection(&mut socket, &resp);
                    drop((resp, slot));
                    log_io(&res);
                });
            }
            .in_current_span(),
        );
    }
}

struct Deferred {
    stream: UnixStream,
    comp: ConnectionWork,
    lease: OwnedSemaphorePermit,
    req: UdsRequest,
    ctx: AccessContext,
    call: ops::CallLabel,
    write: bool,
    started: Instant,
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
