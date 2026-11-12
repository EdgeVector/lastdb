//! Shutdown flush receipt written after request and background writers drain.

use std::io::Write;
use std::path::Path;

use serde::Serialize;

const SHUTDOWN_FLUSH_READY_FILE: &str = ".shutdown_flush_ready";

/// Receipt written when `lastdbd` completes a clean shutdown flush.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ShutdownFlushReceipt {
    pub version: u32,
    pub pid: u32,
    pub start_ts: u64,
    pub flush_ok: bool,
}

/// Remove old proof before a daemon can accept a new mutation.
pub fn clear_shutdown_flush_receipt(home: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(home.join(SHUTDOWN_FLUSH_READY_FILE)) {
        Ok(()) => std::fs::File::open(home)?.sync_all(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Write proof after request and background writers stop and the final flush succeeds.
pub fn write_shutdown_flush_receipt(
    home: &Path,
    pid: u32,
    start_ts: u64,
) -> std::io::Result<ShutdownFlushReceipt> {
    if pid == 0 || start_ts == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid shutdown flush receipt",
        ));
    }
    let receipt = ShutdownFlushReceipt {
        version: 1,
        pid,
        start_ts,
        flush_ok: true,
    };
    let nonce = fold_db::clock::unix_nanos_wide();
    let tmp = home.join(format!(".shutdown_flush_ready.tmp.{pid}.{nonce}"));
    let bytes = serde_json::to_vec(&receipt).map_err(std::io::Error::other)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&tmp, home.join(SHUTDOWN_FLUSH_READY_FILE))?;
    std::fs::File::open(home)?.sync_all()?;
    Ok(receipt)
}
