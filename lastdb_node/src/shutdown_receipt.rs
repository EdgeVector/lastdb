//! Shutdown flush receipt written after request and background writers drain.

use std::io::{Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

const SHUTDOWN_FLUSH_READY_FILE: &str = ".shutdown_flush_ready";

/// Receipt written when `lastdbd` completes a clean shutdown flush.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShutdownFlushReceipt {
    pub version: u32,
    pub pid: u32,
    pub start_ts: u64,
    pub flush_ok: bool,
}

impl ShutdownFlushReceipt {
    fn validate(&self) -> std::io::Result<()> {
        if self.version == 1 && self.pid != 0 && self.start_ts != 0 && self.flush_ok {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid shutdown flush receipt",
            ))
        }
    }
}

/// Read and validate the shutdown flush receipt from `home`.
pub fn read_shutdown_flush_receipt(home: &Path) -> std::io::Result<ShutdownFlushReceipt> {
    let mut bytes = Vec::new();
    std::fs::File::open(home.join(SHUTDOWN_FLUSH_READY_FILE))?
        .take(4097)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "shutdown flush receipt is too large",
        ));
    }
    let receipt: ShutdownFlushReceipt = serde_json::from_slice(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    receipt.validate()?;
    Ok(receipt)
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
    let receipt = ShutdownFlushReceipt {
        version: 1,
        pid,
        start_ts,
        flush_ok: true,
    };
    receipt.validate()?;
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
