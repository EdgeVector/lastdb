//! Owner and full-surface socket serving: request routing, the accept
//! handler shared by both sockets, and worker-pool backpressure logging.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use fold_db::access::{AccessContext, CallerTransport, CallerVerification};
use lastdb_node::exec;
use lastdb_node::host::Host;
use lastdb_uds::uds::UdsSocket;
use lastdb_uds::uds_http::{self, UdsRequest, UdsResponse};
use lastdb_uds::uds_router::{self, SocketKind};
use lastdb_uds::worker_pool::{SubmitError, UdsWorkerPool};

/// Which listener a connection arrived on.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Surface {
    /// The narrow data socket.
    Owner,
    /// The full-surface setup socket: the data surface plus `schemas/load`.
    Full,
}

impl Surface {
    fn label(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Full => "full",
        }
    }
}

/// Shared state every accept handler needs.
#[derive(Clone)]
pub(crate) struct Server {
    pub(crate) host: Arc<Host>,
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) worker_pool: UdsWorkerPool,
}

/// Collect this UDS worker's heap after one request, before the next job.
fn collect_finished_request_heap() {
    lastdb_node::allocator::collect_request_heap_if_over_slack();
}

/// App verbs served on both sockets. `None` means "not one of these".
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
        ("POST", Some("/api/apps/verify-distribution-ready")) => Some(exec::block_on_route(
            handle,
            ctx,
            exec::execute_apps_verify_distribution_ready_route(req, ctx, host),
        )),
        ("POST", Some("/api/apps/shared-surface/publish-attach")) => Some(exec::block_on_route(
            handle,
            ctx,
            exec::execute_apps_shared_surface_publish_attach_route(req, ctx, host),
        )),
        ("GET", Some("/api/apps/shared-surface/attachments")) => Some(exec::block_on_route(
            handle,
            ctx,
            exec::execute_apps_shared_surface_attachments_route(req, ctx, host),
        )),
        _ => None,
    }
}

fn route_request(
    surface: Surface,
    handle: &tokio::runtime::Handle,
    host: &Arc<Host>,
    req: &UdsRequest,
    ctx: &AccessContext,
) -> UdsResponse {
    let path = req.target.split(['?', '#']).next();
    // Setup verbs first on the full socket; everything else shares the narrow
    // socket's dispatch table. Local app-schema declaration is also on the
    // owner socket so collapsed Mini nodes can run `brain init` without the
    // schema_service path.
    if surface == Surface::Full && req.method == "POST" && path == Some("/api/schemas/load") {
        return exec::block_on_route(
            handle,
            ctx,
            exec::execute_load_schemas_route(req, ctx, host),
        );
    }
    if let Some(response) = route_app_verb(handle, host, req, ctx, path) {
        return response;
    }
    uds_router::dispatch(
        req,
        ctx,
        SocketKind::Owner,
        |route, request, context| exec::block_on_data_route(handle, route, request, context, host),
        // No browser-pairing surface in the minimal daemon: the mint verb
        // answers 404.
        uds_router::no_pairing_mint,
    )
}

/// The pool already wrote the 503 on `QueueFull` and `ShutDown`; log why.
fn log_submit_error(pool: &UdsWorkerPool, err: SubmitError, surface: Surface) {
    match err {
        SubmitError::QueueFull => {
            tracing::warn!(
                workers = pool.workers(),
                queue_capacity = pool.queue_capacity(),
                in_flight = pool.in_flight(),
                rejects = pool.queue_full_rejects(),
                socket = surface.label(),
                "uds worker queue full; rejecting peer with 503"
            );
        }
        SubmitError::ShutDown => {
            tracing::error!(
                socket = surface.label(),
                "uds worker pool shut down; rejecting peer"
            );
        }
    }
}

impl Server {
    fn on_accept(
        &self,
        surface: Surface,
        stream: std::os::unix::net::UnixStream,
        transport: CallerTransport,
        verification: CallerVerification,
    ) {
        let host = Arc::clone(&self.host);
        let handle = self.handle.clone();
        let owner_user_id = host.user_hash.clone();
        let submitted = self
            .worker_pool
            .try_submit_connection(stream, move |mut stream| {
                let outcome = uds_http::serve_connection(
                    &mut stream,
                    &owner_user_id,
                    SocketKind::Owner,
                    transport,
                    verification,
                    |req, ctx| route_request(surface, &handle, &host, req, ctx),
                );
                // Same thread that ran the request. Do not collect another
                // worker. Under the slack line this is a no-op.
                collect_finished_request_heap();
                if let Err(e) = outcome {
                    match surface {
                        Surface::Owner => {
                            tracing::debug!(error = %e, "control-socket connection ended with error");
                        }
                        Surface::Full => {
                            tracing::debug!(error = %e, "full-socket connection ended with error");
                        }
                    }
                }
            });
        if let Err(err) = submitted {
            log_submit_error(&self.worker_pool, err, surface);
        }
    }

    /// Run the accept loop for one socket until `shutdown` is set.
    ///
    /// No macOS code-signature verifier in the minimal daemon: every
    /// same-user peer keeps the base `Unverified` posture, which the owner
    /// socket maps to owner context (the device-trust model the full node
    /// applies on this socket today).
    pub(crate) fn serve(
        &self,
        surface: Surface,
        socket: &UdsSocket,
        owner_uid: u32,
        shutdown: &AtomicBool,
    ) -> std::io::Result<()> {
        socket.serve(
            owner_uid,
            shutdown,
            |_handle| CallerVerification::Unverified,
            |stream, transport, verification| {
                self.on_accept(surface, stream, transport, verification);
            },
        )
    }
}
