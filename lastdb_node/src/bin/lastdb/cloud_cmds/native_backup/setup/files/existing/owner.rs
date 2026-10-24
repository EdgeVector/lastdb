//! Bounded read-only checks against the existing owner daemon, not a new runtime API.

use serde_json::Value;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[path = "owner/socket.rs"]
mod socket;

pub(super) struct Owner {
    home: PathBuf,
    account: String,
    socket_identity: (u64, u64),
    process_start: u64,
}

impl Owner {
    pub(super) fn open(home: &Path, account: &str) -> Result<Self, String> {
        let socket_identity = socket_identity(home)?;
        let process_start = probe(home, account)?;
        let owner = Self {
            home: home.into(),
            account: account.into(),
            socket_identity,
            process_start,
        };
        owner.check_socket()?;
        Ok(owner)
    }

    pub(super) fn check_socket(&self) -> Result<(), String> {
        if socket_identity(&self.home)? != self.socket_identity {
            return Err("owner socket changed during native enrollment".into());
        }
        Ok(())
    }

    pub(super) fn verify(&self) -> Result<(), String> {
        self.check_socket()?;
        if probe(&self.home, &self.account)? != self.process_start {
            return Err("owner daemon changed during native enrollment".into());
        }
        self.check_socket()
    }
}

fn socket_identity(home: &Path) -> Result<(u64, u64), String> {
    let info = std::fs::symlink_metadata(home.join("data/folddb.sock"))
        .map_err(|_| "existing enrollment requires its live owner socket")?;
    if !info.file_type().is_socket() || info.uid() != unsafe { libc::geteuid() } {
        return Err("native enrollment needs an owner-held socket".into());
    }
    Ok((info.dev(), info.ino()))
}

fn probe(home: &Path, account: &str) -> Result<u64, String> {
    let (status, automatic) = std::thread::scope(|scope| {
        let status = scope.spawn(|| request(home, "GET", "/api/status", ""));
        let automatic = scope.spawn(|| {
            request(
                home,
                "POST",
                "/api/sync/native-backup",
                r#"{"action":"auto_status"}"#,
            )
        });
        Ok::<_, String>((
            status.join().map_err(|_| "owner status task failed")??,
            automatic
                .join()
                .map_err(|_| "owner backup status task failed")??,
        ))
    })?;
    let home_text = home.to_str().ok_or("existing home is not UTF-8")?;
    let data = home.join("data");
    if status["ok"] != true
        || status["user_hash"].as_str() != Some(account)
        || status["status"]["home_path_disclosed"] != true
        || status["status"]["home"].as_str() != Some(home_text)
        || status["status"]["data_dir_path_disclosed"] != true
        || status["status"]["data_dir"].as_str() != data.to_str()
        || status["status"]["sync"]["enabled"] != false
        || automatic["ok"] != true
        || automatic["user_hash"].as_str() != Some(account)
        || automatic["enabled"] != false
        || automatic["backup"]["enabled"] != false
        || automatic["backup"]["running"] != false
    {
        return Err(
            "owner identity, home, or disabled backup state does not match enrollment".into(),
        );
    }
    status["status"]["process_start_ts"]
        .as_u64()
        .filter(|start| *start > 0)
        .ok_or_else(|| "owner process identity is unavailable".into())
}

fn request(home: &Path, method: &str, target: &str, body: &str) -> Result<Value, String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut stream = socket::connect(&home.join("data/folddb.sock"), deadline)?;
    let request = format!("{method} {target} HTTP/1.1\r\nHost: localhost\r\nX-LastDB-Client: lastdb\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    socket::write_all(&mut stream, request.as_bytes(), deadline)?;
    let limit = cloud_backup_protocol::limits::REQUEST_JSON_MAX_BYTES as usize + 16 * 1024;
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let count = socket::read(&mut stream, &mut chunk, deadline)?;
        if count == 0 {
            break;
        }
        if count > limit.saturating_sub(bytes.len()) {
            return Err("owner status reply exceeds its bound".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| "invalid owner status encoding")?;
    if !text.starts_with("HTTP/1.1 200 ") {
        return Err("owner status request refused".into());
    }
    let (_, body) = text
        .split_once("\r\n\r\n")
        .ok_or("invalid owner status response")?;
    serde_json::from_str(body).map_err(|_| "invalid owner status JSON".into())
}
