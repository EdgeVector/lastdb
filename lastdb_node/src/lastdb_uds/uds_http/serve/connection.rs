//! Split bounded framing from handler work.
use super::*;

/// Parse one request with the connection deadline and the context gates.
/// Malformed requests receive their original content-free response here.
pub fn receive_connection(
    stream: &mut std::os::unix::net::UnixStream,
    owner_user_id: &str,
) -> io::Result<Option<(UdsRequest, AccessContext)>> {
    receive_with_budget(stream, owner_user_id, UDS_CONN_BUDGET)
}

pub(super) fn receive_with_budget(
    stream: &mut std::os::unix::net::UnixStream,
    owner_user_id: &str,
    budget: std::time::Duration,
) -> io::Result<Option<(UdsRequest, AccessContext)>> {
    let read_half = stream.try_clone()?;
    let deadline = std::time::Instant::now() + budget;
    let base = uds_access_context(owner_user_id);
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
    Ok(Some((request, base)))
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
