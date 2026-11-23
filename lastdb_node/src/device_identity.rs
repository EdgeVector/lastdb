//! Stable sync device id under `<home>/log-sync/device-id`.
//!
//! It is random, made once for an installation, and names the stream that the
//! installation signs. The host reads it when it opens the store, because each
//! captured write is stamped with it at its commit, and the sync code reads it
//! for every entry it signs.

use std::path::Path;

use mutation_log::DeviceId;
use rand::RngCore;

use crate::profile::private_fs as disk;

const LIMIT: usize = 4096;

/// Fresh random 32-byte device stream id.
fn random_device_id() -> DeviceId {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    DeviceId::from_bytes(bytes)
}

pub(crate) fn device_id(home: &Path) -> Result<DeviceId, String> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    let _guard = LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let path = home.join("log-sync").join("device-id");
    if let Some(bytes) = disk::read_limited(&path, LIMIT).map_err(|_| "cannot read device id")? {
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| "device id is not text")?
            .trim();
        return DeviceId::parse(text).map_err(|_| "device id is not 64 lowercase hex".into());
    }
    let fresh = random_device_id();
    disk::write_private(&path, format!("{fresh}\n").as_bytes())
        .map_err(|_| "cannot write device id")?;
    Ok(fresh)
}
