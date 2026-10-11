//! Bounded UploadPart file streams; the service alone completes the upload.

use super::protocol::CloudPurgeUploadPart;
use crate::sync::{SyncError, SyncResult};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::{Command, Stdio};

fn escape(value: &str) -> SyncResult<String> {
    if value.contains(['\n', '\r', '\0']) {
        return Err(SyncError::Storage("invalid source upload grant".into()));
    }
    Ok(value.replace('\\', "\\\\").replace('"', "\\\""))
}

pub(super) fn upload_part(path: &Path, part: &CloudPurgeUploadPart) -> SyncResult<String> {
    if !reqwest::Url::parse(&part.url).is_ok_and(|url| url.scheme() == "https") {
        return Err(SyncError::Storage("source upload requires HTTPS".into()));
    }
    let mut config = tempfile::NamedTempFile::new()?;
    writeln!(config, "url = \"{}\"", escape(&part.url)?)?;
    for (name, value) in &part.headers {
        if !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(SyncError::Storage(
                "invalid source upload header name".into(),
            ));
        }
        writeln!(config, "header = \"{}: {}\"", name, escape(value)?)?;
    }
    config.flush()?;
    let headers = tempfile::NamedTempFile::new()?;
    let mut source = std::fs::File::open(path)?;
    source.seek(SeekFrom::Start(part.offset))?;
    let mut child = Command::new("curl")
        .args([
            "-sS",
            "-f",
            "--proto",
            "=https",
            "--proto-redir",
            "=https",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--connect-timeout",
            "30",
            "--speed-limit",
            "1",
            "--speed-time",
            "90",
            "--http1.1",
            "-X",
            "PUT",
            "-T",
            "-",
            "-H",
            "Transfer-Encoding:",
            "-H",
            "Expect:",
        ])
        .arg("-H")
        .arg(format!("Content-Length: {}", part.bytes))
        .arg("-K")
        .arg(config.path())
        .arg("--dump-header")
        .arg(headers.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let copied = {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| SyncError::Storage("source upload pipe absent".into()))?;
        std::io::copy(&mut source.take(part.bytes), &mut stdin)
    };
    let output = child.wait_with_output()?;
    let code = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() || !code.starts_with('2') {
        return Err(SyncError::Network(
            "retained source part upload failed".into(),
        ));
    }
    if copied? != part.bytes {
        return Err(SyncError::Storage(
            "retained source part became shorter".into(),
        ));
    }
    if headers.as_file().metadata()?.len() > 65536 {
        return Err(SyncError::Network(
            "source part response headers exceed the limit".into(),
        ));
    }
    let bytes = std::fs::read(headers.path())?;
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("etag")
                .then(|| value.trim().to_string())
        })
        .rfind(|value| !value.is_empty())
        .ok_or_else(|| SyncError::Storage("source upload part has no provider ETag".into()))
}
