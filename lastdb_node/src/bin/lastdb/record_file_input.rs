//! Send a record mutation and its file in one request.

use super::*;

const PUT_BLOB_PATH: &str = "/api/db/put-blob";

/// The raw HTTP request: the bytes are the body, so there is no base64 tax.
/// The complete record and file description are a length-prefixed JSON
/// segment before the raw bytes, so a record can exceed the header limit.
fn put_blob_request(bytes: &[u8], metadata: &serde_json::Value) -> Result<Vec<u8>, String> {
    admit_local_file_input(bytes.len() as u64)?;
    let json = serde_json::to_vec(metadata).map_err(|e| format!("serialize file metadata: {e}"))?;
    let body_length = json
        .len()
        .checked_add(bytes.len())
        .ok_or("local file request length overflow")?;
    let head = format!(
        "POST {PUT_BLOB_PATH} HTTP/1.1\r\n\
         Host: localhost\r\n\
         {}Content-Type: application/octet-stream\r\n\
         X-LastDB-File-Blob-Metadata-Length: {}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        client_headers(),
        json.len(),
        body_length
    );
    let capacity = head
        .len()
        .checked_add(body_length)
        .ok_or_else(|| "local file request length overflow".to_string())?;
    admit_local_file_input(capacity as u64)?;
    let mut request = Vec::new();
    request
        .try_reserve_exact(capacity)
        .map_err(|_| "local file request allocation refused".to_string())?;
    request.extend_from_slice(head.as_bytes());
    request.extend_from_slice(&json);
    request.extend_from_slice(bytes);
    Ok(request)
}

fn admit_local_file_input(bytes: u64) -> Result<(), String> {
    if bytes > lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err("local file input exceeds native pending memory allowance".into());
    }
    Ok(())
}

fn read_local_file_input(mut input: impl std::io::Read) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = input
            .read(&mut buffer)
            .map_err(|e| format!("read local file input: {e}"))?;
        if count == 0 {
            return Ok(bytes);
        }
        let length = bytes
            .len()
            .checked_add(count)
            .ok_or_else(|| "local file input length overflow".to_string())?;
        admit_local_file_input(length as u64)?;
        if length > bytes.capacity() {
            let maximum =
                lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES as usize;
            let capacity = bytes.capacity().saturating_mul(2).max(length).min(maximum);
            bytes
                .try_reserve_exact(capacity - bytes.len())
                .map_err(|_| "local file input allocation refused".to_string())?;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}

pub(crate) fn put_record_file(
    socket: &Path,
    metadata: &serde_json::Value,
    source: &Path,
) -> Result<serde_json::Value, String> {
    let input = std::fs::File::open(source)
        .map_err(|error| format!("open {}: {error}", source.display()))?;
    admit_local_file_input(
        input
            .metadata()
            .map_err(|error| format!("file metadata: {error}"))?
            .len(),
    )?;
    let bytes = read_local_file_input(input)?;
    let request = put_blob_request(&bytes, metadata)?;
    drop(bytes);
    let response = request_with_timeout(socket, &request, admin_scan_client_timeout())?;
    parse_json_response(&response, PUT_BLOB_PATH)
}
