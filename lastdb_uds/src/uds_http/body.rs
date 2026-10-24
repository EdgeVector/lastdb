//! Exact Content-Length framing with trusted owner raw-file admission.

use super::{io, Read, UdsHttpError, MAX_BODY_LEN};

pub(super) fn read(
    reader: &mut impl Read,
    headers: &[(String, String)],
    method: &str,
    target: &str,
    allow_owner_blob: bool,
) -> Result<Vec<u8>, UdsHttpError> {
    let raw_blob = allow_owner_blob
        && method == "POST"
        && target == "/api/db/put-blob-local"
        && headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .and_then(|(_, value)| value.split(';').next())
            .is_some_and(|value| {
                value
                    .trim()
                    .eq_ignore_ascii_case("application/octet-stream")
            });
    // Reject impossible inputs before allocation. The stored representation,
    // pending-write memory, and disk admission still decide the complete write.
    let maximum = if raw_blob {
        lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
    } else {
        MAX_BODY_LEN as u64
    };
    let content_length = resolve_content_length(headers, maximum)?;
    let mut body = Vec::new();
    body.try_reserve_exact(content_length)
        .map_err(|_| UdsHttpError::Io(io::Error::other("request body allocation refused")))?;
    body.resize(content_length, 0);
    reader
        .read_exact(&mut body)
        .map_err(|error| match error.kind() {
            io::ErrorKind::UnexpectedEof => UdsHttpError::UnexpectedEof,
            _ => UdsHttpError::Io(error),
        })?;
    Ok(body)
}

/// Refuse contradictory lengths before allocation; absent Content-Length is zero.
fn resolve_content_length(
    headers: &[(String, String)],
    maximum: u64,
) -> Result<usize, UdsHttpError> {
    let mut length = 0usize;
    let mut seen = false;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("content-length") {
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| UdsHttpError::InvalidContentLength)?;
            if seen && parsed != length {
                return Err(UdsHttpError::InvalidContentLength);
            }
            length = parsed;
            seen = true;
        }
    }
    if length as u64 > maximum {
        return Err(UdsHttpError::BodyTooLarge);
    }
    Ok(length)
}
