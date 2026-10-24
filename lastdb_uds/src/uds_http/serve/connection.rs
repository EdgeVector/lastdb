//! Split bounded framing from handler work without changing access posture.
use super::*;

/// Parse one request with the same deadline and context gates as serve_connection.
/// Malformed requests receive their original content-free response here.
pub fn receive_connection(
    stream: &mut std::os::unix::net::UnixStream,
    owner_user_id: &str,
    socket_kind: SocketKind,
    transport: CallerTransport,
    verification: CallerVerification,
) -> io::Result<Option<(UdsRequest, AccessContext)>> {
    receive_with_budget(
        stream,
        owner_user_id,
        socket_kind,
        transport,
        verification,
        UDS_CONN_BUDGET,
    )
}

pub(super) fn receive_with_budget(
    stream: &mut std::os::unix::net::UnixStream,
    owner_user_id: &str,
    socket_kind: SocketKind,
    transport: CallerTransport,
    verification: CallerVerification,
    budget: std::time::Duration,
) -> io::Result<Option<(UdsRequest, AccessContext)>> {
    let read_half = stream.try_clone()?;
    let deadline = std::time::Instant::now() + budget;
    let base = uds_access_context(owner_user_id, socket_kind, transport, verification);
    // Only the authenticated owner may stream a large local file upload.
    // Preserve that framing rule for both immediate and deferred handlers.
    let request = match read_request_with_owner_blob(
        io::BufReader::new(DeadlineStream::new(&read_half, deadline)),
        base.is_owner,
    ) {
        Ok(request) => request,
        Err(UdsHttpError::Empty | UdsHttpError::Io(_)) => return Ok(None),
        Err(error) => {
            finish_with_budget(stream, &error_response(&error), budget)?;
            return Ok(None);
        }
    };
    let peer_pid = peer_pid_from_stream(stream);
    let base = base.with_peer_pid(peer_pid);
    let context = match base.with_db_handle(request.header(fold_db::access::LASTDB_DB_HEADER)) {
        Ok(context) => context,
        Err(message) => {
            let response = UdsResponse::new(400, "Bad Request", message.into_bytes())
                .with_header("Content-Type", "text/plain; charset=utf-8");
            finish_with_budget(stream, &response, budget)?;
            return Ok(None);
        }
    };
    Ok(Some((request, context)))
}

/// Write one final response with a fresh deadline and an explicit EOF.
pub fn finish_connection(
    stream: &mut std::os::unix::net::UnixStream,
    response: &UdsResponse,
) -> io::Result<()> {
    finish_with_budget(stream, response, UDS_CONN_BUDGET)
}

pub(super) fn finish_with_budget(
    stream: &mut std::os::unix::net::UnixStream,
    response: &UdsResponse,
    budget: std::time::Duration,
) -> io::Result<()> {
    let deadline = std::time::Instant::now() + budget;
    write_response(&mut DeadlineStream::new(stream, deadline), response)?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}
