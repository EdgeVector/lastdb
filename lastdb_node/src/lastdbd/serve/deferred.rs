//! Only mutation handler waits leave the bounded socket worker.
use super::*;
use lastdb_uds::worker_pool::ConnectionWork;
use std::os::unix::net::UnixStream;

pub(super) fn serve(
    server: &Server,
    surface: Surface,
    mut stream: UnixStream,
    transport: CallerTransport,
    verification: CallerVerification,
    completion: ConnectionWork,
) -> std::io::Result<()> {
    // Reserve before allocation, not after a parsed body enters another queue.
    let mut body = server.body_budget.reserve(&server.handle)?;
    let Some((request, context)) = uds_http::receive_connection(
        &mut stream,
        &server.host.user_hash,
        SocketKind::Owner,
        transport,
        verification,
    )?
    else {
        return Ok(());
    };
    let route = uds_router::route(&request, SocketKind::Owner);
    if let uds_router::ControlRoute::Data(
        route @ (uds_router::DataRoute::Mutation | uds_router::DataRoute::MutationBatch),
    ) = route
    {
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
                surface,
            );
            return Ok(());
        }
    }
    let response = route_request(surface, &server.handle, &server.host, &request, &context);
    uds_http::finish_connection(&mut stream, &response)
}

struct Pending {
    stream: UnixStream,
    request: UdsRequest,
    context: AccessContext,
    body: body_budget::BodyLease,
    completion: ConnectionWork,
}

fn defer(server: &Server, pending: Pending, route: uds_router::DataRoute, surface: Surface) {
    let host = Arc::clone(&server.host);
    server.handle.spawn(async move {
        let Pending {
            stream,
            request,
            context,
            body,
            completion,
        } = pending;
        let response = exec::with_socket_queue_wait(
            completion.queue_wait(),
            exec::execute_data_route_with_timeout(route, &request, &context, &host),
        )
        .await;
        // Release frame bytes before scheduling the response. Parsers that wait
        // for byte capacity must not prevent these responses from reaching a worker.
        drop(request);
        drop(body);
        completion.resume(stream, move |mut stream| {
            let result = uds_http::finish_connection(&mut stream, &response);
            drop(response);
            log_result(result, surface);
            collect_finished_request_heap();
        });
    });
}

pub(super) fn log_result(result: std::io::Result<()>, surface: Surface) {
    if let Err(error) = result {
        tracing::debug!(error = %error, socket = surface.label(), "socket connection ended with error");
    }
}
