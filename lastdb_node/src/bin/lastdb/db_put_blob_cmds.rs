//! `lastdb db put-blob`: store the file of one record with the record.

use super::*;

const PUT_BLOB_PATH: &str = "/api/db/put-blob";

/// Arguments of `lastdb db put-blob`.
#[derive(clap::Args, Debug)]
pub(crate) struct PutBlobArgs {
    /// Complete conditional record request as JSON.
    #[arg(long)]
    pub(crate) request_file: PathBuf,
    /// File with the bytes to store. Omit it to read stdin.
    #[arg(long)]
    pub(crate) file: Option<PathBuf>,
    /// Optional name of the file, kept in the record's file marker.
    #[arg(long)]
    pub(crate) name: Option<String>,
    /// Optional media type of the file, kept in the record's file marker.
    #[arg(long)]
    pub(crate) media_type: Option<String>,
}

/// The raw HTTP request: the bytes are the body, so there is no base64 tax.
/// The record and the file description ride as compact JSON in a header.
fn put_blob_request(bytes: &[u8], metadata: &serde_json::Value) -> Result<Vec<u8>, String> {
    admit_local_file_input(bytes.len() as u64)?;
    let json =
        serde_json::to_string(metadata).map_err(|e| format!("serialize blob metadata: {e}"))?;
    let head = format!(
        "POST {PUT_BLOB_PATH} HTTP/1.1\r\n\
         Host: localhost\r\n\
         {}Content-Type: application/octet-stream\r\n\
         X-LastDB-File-Blob-Metadata: {json}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        client_headers(),
        bytes.len()
    );
    let capacity = head
        .len()
        .checked_add(bytes.len())
        .ok_or_else(|| "local file request length overflow".to_string())?;
    admit_local_file_input(capacity as u64)?;
    let mut request = Vec::new();
    request
        .try_reserve_exact(capacity)
        .map_err(|_| "local file request allocation refused".to_string())?;
    request.extend_from_slice(head.as_bytes());
    request.extend_from_slice(bytes);
    Ok(request)
}

/// The header metadata of one put: the record, the file description and the
/// other fields.
fn put_blob_metadata(args: &PutBlobArgs) -> Result<serde_json::Value, String> {
    let raw =
        std::fs::read(&args.request_file).map_err(|e| format!("read complete record: {e}"))?;
    let mutation: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|e| format!("parse complete record: {e}"))?;
    let mut metadata = serde_json::json!({"mutation": mutation});
    if let Some(name) = &args.name {
        metadata["name"] = name.as_str().into();
    }
    if let Some(media_type) = &args.media_type {
        metadata["media_type"] = media_type.as_str().into();
    }
    Ok(metadata)
}

pub(crate) fn read_put_blob_input(file: Option<&Path>) -> Result<Vec<u8>, String> {
    use std::io::IsTerminal;

    if let Some(path) = file {
        let input =
            std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
        admit_local_file_input(
            input
                .metadata()
                .map_err(|e| format!("file metadata: {e}"))?
                .len(),
        )?;
        return read_local_file_input(input);
    }
    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        return Err("provide --file PATH, or pipe the bytes on stdin".to_string());
    }
    read_local_file_input(stdin.lock())
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

pub(crate) fn db_put_blob(socket: &Path, args: &PutBlobArgs) -> Result<(), String> {
    let metadata = put_blob_metadata(args)?;
    let report = put_record_file(socket, &metadata, args.file.as_deref())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|e| format!("serialize report: {e}"))?
    );
    Ok(())
}

pub(crate) fn put_record_file(
    socket: &Path,
    metadata: &serde_json::Value,
    source: Option<&Path>,
) -> Result<serde_json::Value, String> {
    let bytes = read_put_blob_input(source)?;
    let request = put_blob_request(&bytes, metadata)?;
    drop(bytes);
    let response = request_with_timeout(socket, &request, admin_scan_client_timeout())?;
    parse_json_response(&response, PUT_BLOB_PATH)
}
