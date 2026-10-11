//! Stream large uploads without exposing signed URLs in process arguments.

use super::*;
use std::io::Write;
use std::process::Output;

pub(super) struct CurlUploadOutput {
    pub output: Output,
    pub etag: Option<String>,
}

fn upload_headers() -> SyncResult<tempfile::NamedTempFile> {
    tempfile::NamedTempFile::new().map_err(SyncError::Io)
}

fn finish_output(
    output: Output,
    headers: &tempfile::NamedTempFile,
) -> SyncResult<CurlUploadOutput> {
    if headers.as_file().metadata().map_err(SyncError::Io)?.len() > 65536 {
        return Err(SyncError::Network(
            "upload response headers are too large".into(),
        ));
    }
    let bytes = std::fs::read(headers.path()).map_err(SyncError::Io)?;
    let etag = String::from_utf8_lossy(&bytes)
        .lines()
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("etag")
                .then(|| value.trim().to_string())
        })
        .next_back();
    Ok(CurlUploadOutput { output, etag })
}

/// Escape a value for curl `--config` double-quoted strings.
fn curl_config_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Build argv for the large-object curl PUT. The presigned URL is **not** on
/// argv — it is only referenced via `-K <config>` so process listings cannot
/// harvest signature query material.
fn curl_large_put_argv(config_path: &Path, file_path: &Path, max_time_secs: u64) -> Vec<String> {
    vec![
        "-sS".to_string(),
        "-f".to_string(),
        "--proto".to_string(),
        "=https".to_string(),
        "--proto-redir".to_string(),
        "=https".to_string(),
        "-o".to_string(),
        "/dev/null".to_string(),
        "-w".to_string(),
        "%{http_code}".to_string(),
        "--max-time".to_string(),
        max_time_secs.to_string(),
        "-X".to_string(),
        "PUT".to_string(),
        "-H".to_string(),
        "Content-Type: application/octet-stream".to_string(),
        "-T".to_string(),
        file_path.display().to_string(),
        "-K".to_string(),
        config_path.display().to_string(),
    ]
}

/// Blocking curl PUT used by [`S3Client::upload_snapshot_file_via_curl`].
///
/// Writes the presigned URL into a temporary curl config file and invokes curl
/// with only the config path on argv (never the raw URL).
pub(super) fn upload_snapshot_file_via_curl_blocking(
    url: &str,
    path: &Path,
    max_time_secs: u64,
) -> SyncResult<CurlUploadOutput> {
    let config = curl_upload_config(url)?;
    let headers = upload_headers()?;
    let mut args = curl_large_put_argv(config.path(), path, max_time_secs);
    args.extend(["--dump-header".into(), headers.path().display().to_string()]);
    // Defensive: never allow a bare URL-looking token onto argv.
    for arg in &args {
        if arg.starts_with("http://") || arg.starts_with("https://") {
            return Err(SyncError::Network(
                "S3 upload: internal error — refused to put URL on curl argv".to_string(),
            ));
        }
    }
    let output = std::process::Command::new("curl")
        .args(&args)
        .output()
        .map_err(|e| {
            SyncError::Network(format!(
                "S3 upload: failed to spawn curl (is curl on PATH?): {e}"
            ))
        })?;
    finish_output(output, &headers)
}

fn curl_upload_config(url: &str) -> SyncResult<tempfile::NamedTempFile> {
    if url.chars().any(char::is_control)
        || !reqwest::Url::parse(url).is_ok_and(|parsed| parsed.scheme() == "https")
    {
        return Err(SyncError::Network("invalid cloud upload URL".into()));
    }
    let mut config = tempfile::NamedTempFile::new().map_err(|e| {
        SyncError::Network(format!(
            "S3 upload: failed to create curl config tempfile: {e}"
        ))
    })?;
    // curl --config: double-quoted string value for `url`.
    writeln!(config, "url = \"{}\"", curl_config_escape(url))
        .map_err(|e| SyncError::Network(format!("S3 upload: failed to write curl config: {e}")))?;
    config
        .flush()
        .map_err(|e| SyncError::Network(format!("S3 upload: failed to flush curl config: {e}")))?;

    Ok(config)
}

/// Keep the large-object curl transport while streaming only the verified
/// prefix from the retained handle. The stdin pipe is bounded; no chunk copy
/// is written to disk or held in RAM. The URL remains in the config file.
pub(super) fn upload_backup_prefix_via_curl_blocking(
    url: &str,
    source: std::fs::File,
    bytes: u64,
    max_time_secs: u64,
) -> SyncResult<CurlUploadOutput> {
    use std::io::Read;
    use std::process::Stdio;

    let config = curl_upload_config(url)?;
    let headers = upload_headers()?;
    let mut args = curl_large_put_argv(config.path(), Path::new("-"), max_time_secs);
    args.extend(["--dump-header".into(), headers.path().display().to_string()]);
    args.extend([
        "--http1.1".to_string(),
        "-H".to_string(),
        format!("Content-Length: {bytes}"),
        "-H".to_string(),
        "Transfer-Encoding:".to_string(),
        "-H".to_string(),
        "Expect:".to_string(),
    ]);
    let mut child = std::process::Command::new("curl")
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(SyncError::Io)?;
    let copied = {
        let mut stdin = child.stdin.take().expect("piped curl stdin");
        std::io::copy(&mut source.take(bytes), &mut stdin)
    };
    let output = child.wait_with_output().map_err(SyncError::Io)?;
    // Preserve curl's HTTP/transport error if it closed the pipe early.
    if output.status.success() && copied? != bytes {
        return Err(SyncError::Storage(
            "backup source became shorter during upload".into(),
        ));
    }
    finish_output(output, &headers)
}

pub(super) fn check_curl_upload_output(output: &Output) -> SyncResult<()> {
    let http_code = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = redact_sync_error_text(&stderr);
    if output.status.success()
        && http_code.len() == 3
        && http_code.starts_with('2')
        && http_code.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Ok(());
    }
    Err(SyncError::Network(format!(
        "S3 upload via curl failed: exit={} http_code={http_code} stderr={stderr}",
        output.status.code().unwrap_or(-1)
    )))
}
