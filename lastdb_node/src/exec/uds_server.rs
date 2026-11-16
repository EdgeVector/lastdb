//! Owner-socket request framing, body-budget admission, and worker dispatch.

use std::os::unix::net::UnixStream;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Instrument;

use super::{block_on_data_route, block_on_route, database_routes, database_session, mutation};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{self, UdsRequest, UdsResponse};
use crate::lastdb_uds::uds_router::{self, ControlRoute, DataRoute};
use crate::lastdb_uds::{ConnectionWork, SubmitError, UdsSocket, UdsWorkerPool};

const MAX_PARSE_BYTES: usize =
    uds_http::MAX_BODY_LEN + 4 * uds_http::MAX_HEADERS_TOTAL + 4 * uds_http::MAX_LINE_LEN;
const PARSE_PERMITS: u32 = MAX_PARSE_BYTES as u32;

#[derive(Clone)]
pub struct UdsServer {
    host: Arc<Host>,
    handle: tokio::runtime::Handle,
    pool: UdsWorkerPool,
    body_budget: Arc<Semaphore>,
    mutation_replies: Arc<Semaphore>,
}

impl UdsServer {
    #[must_use]
    pub fn new(host: Arc<Host>, handle: tokio::runtime::Handle, pool: UdsWorkerPool) -> Self {
        let snapshot = pool.snapshot();
        let permits = snapshot
            .workers
            .saturating_mul(MAX_PARSE_BYTES)
            .min(Semaphore::MAX_PERMITS);
        Self {
            host,
            handle,
            pool,
            body_budget: Arc::new(Semaphore::new(permits)),
            mutation_replies: Arc::new(Semaphore::new(snapshot.workers)),
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
        if let Ok(lease) = Arc::clone(&self.body_budget).try_acquire_many_owned(PARSE_PERMITS) {
            return self.serve_reserved(stream, comp, lease);
        }
        let server = self.clone();
        self.handle.spawn(
            async move {
                match Arc::clone(&server.body_budget)
                    .acquire_many_owned(PARSE_PERMITS)
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
        let Some((mut req, ctx)) = uds_http::receive_connection(&mut stream, &self.host.user_hash)?
        else {
            return Ok(());
        };
        if !database_session::admit(&req, &self.host) {
            let response = UdsResponse::new(403, "Forbidden", b"database session refused".to_vec());
            return uds_http::finish_connection(&mut stream, &response);
        }
        let routed = uds_router::route(&req);
        let deferred = match &routed {
            ControlRoute::Data(r @ (DataRoute::Mutation | DataRoute::MutationBatch)) => {
                Some(Some(*r))
            }
            ControlRoute::Database if database_routes::is_deferred_database_request(&req) => {
                Some(None)
            }
            _ => None,
        };
        if let Some(route) = deferred.filter(|_| retain_permits(&mut lease, &req)) {
            let (host, replies) = (Arc::clone(&self.host), Arc::clone(&self.mutation_replies));
            self.handle.spawn(
                async move {
                    let Ok(slot) = replies.acquire_owned().await else {
                        return;
                    };
                    let resp = if let Some(r) = route {
                        mutation::execute_deferred_mutation(r, &req, &ctx, &host).await
                    } else {
                        database_routes::execute_deferred_database_request(&mut req, &ctx, &host)
                            .await
                    };
                    drop((req, lease));
                    comp.resume(stream, move |mut s, _| {
                        let res = uds_http::finish_connection(&mut s, &resp);
                        drop((resp, slot));
                        log_io(&res);
                    });
                }
                .in_current_span(),
            );
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
        uds_http::finish_connection(&mut stream, &resp)
    }
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
