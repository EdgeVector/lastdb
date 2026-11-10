//! Opt-in CI evidence from actual control responses, never provider object requests.
use super::{BackupTransportError, Result};
use reqwest::header::HeaderMap;
use serde_json::{json, Value};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

const RECEIPT_ENV: &str = "LASTDB_CLOUD_CI_SERVICE_RECEIPT";
const MAX_BYTES: u64 = 8 * 1024 * 1024;

pub(super) struct Capture {
    file: File,
    request: u64,
}

fn capture_error() -> BackupTransportError {
    BackupTransportError::Configuration("CI service identity receipt")
}

/// Reserve evidence before sending. A lost response/write leaves an unmatched
/// start, so retries cannot conceal an unobserved serving revision.
pub(super) fn begin() -> Result<Option<Capture>> {
    let Some(path) = std::env::var_os(RECEIPT_ENV) else {
        return Ok(None);
    };
    if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true") {
        return Err(capture_error());
    }
    let path = Path::new(&path);
    if !path.is_absolute() {
        return Err(capture_error());
    }
    let before = std::fs::symlink_metadata(path).map_err(|_| capture_error())?;
    if !before.is_file() || before.file_type().is_symlink() {
        return Err(capture_error());
    }
    // The runner precreates the private receipt. Never create an arbitrary file.
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(path)
        .map_err(|_| capture_error())?;
    let opened = file.metadata().map_err(|_| capture_error())?;
    if !opened.is_file() {
        return Err(capture_error());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened.mode() & 0o077 != 0
            || opened.nlink() != 1
            || (before.dev(), before.ino()) != (opened.dev(), opened.ino())
        {
            return Err(capture_error());
        }
    }
    let request = append(
        &mut file,
        |offset| json!({"event": "started", "request": offset}),
    )?;
    Ok(Some(Capture { file, request }))
}

impl Capture {
    pub(super) fn finish(mut self, response: &reqwest::Response) -> Result<()> {
        let identity = identity(response.headers());
        let request = self.request;
        append(&mut self.file, |_| {
            json!({"event": "response", "request": request,
                   "status": response.status().as_u16(), "identity": identity})
        })?;
        Ok(())
    }
}

fn append(file: &mut File, entry: impl FnOnce(u64) -> Value) -> Result<u64> {
    file.lock().map_err(|_| capture_error())?;
    let result = (|| {
        let offset = file.metadata().map_err(|_| capture_error())?.len();
        let mut bytes = serde_json::to_vec(&entry(offset)).map_err(|_| capture_error())?;
        bytes.push(b'\n');
        if bytes.len() > 512 || offset.saturating_add(bytes.len() as u64) > MAX_BYTES {
            return Err(capture_error());
        }
        file.write_all(&bytes).map_err(|_| capture_error())?;
        Ok(offset)
    })();
    File::unlock(file).map_err(|_| capture_error())?;
    result
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> std::result::Result<Option<&'a str>, ()> {
    let mut values = headers.get_all(name).iter();
    let value = values.next();
    if values.next().is_some() {
        return Err(());
    }
    value
        .map(|value| value.to_str().map_err(|_| ()))
        .transpose()
}

fn revision(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn identity(headers: &HeaderMap) -> Value {
    let values = (
        header(headers, "x-exemem-revision"),
        header(headers, "x-exemem-function-version"),
        header(headers, "x-exemem-lastdb-revision"),
    );
    let (Ok(source), Ok(version), Ok(lastdb)) = values else {
        return json!({"state": "invalid"});
    };
    if source.is_some_and(|value| !revision(value))
        || version.is_some_and(|value| {
            value.is_empty()
                || value.len() > 20
                || value.starts_with('0')
                || !value.bytes().all(|byte| byte.is_ascii_digit())
        })
        || lastdb.is_some_and(|value| !revision(value))
    {
        return json!({"state": "invalid"});
    }
    let (Some(source), Some(version)) = (source, version) else {
        return json!({"state": "missing"});
    };
    json!({"state": "identified", "revision": source,
           "function_version": version, "lastdb_revision": lastdb})
}
