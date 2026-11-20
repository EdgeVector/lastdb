//! Stable sync device id under `<home>/log-sync/device-id`.

use std::path::Path;

use mutation_log::DeviceId;

use crate::profile::private_fs as disk;

const LIMIT: usize = 4096;

pub(super) fn device_id(home: &Path) -> Result<DeviceId, String> {
    let path = home.join("log-sync").join("device-id");
    if let Some(bytes) = disk::read_limited(&path, LIMIT).map_err(|_| "cannot read device id")? {
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| "device id is not text")?
            .trim();
        return DeviceId::parse(text).map_err(|_| "device id is not a UUID".into());
    }
    let fresh = uuid::Uuid::new_v4();
    let text = fresh.to_string();
    disk::write_private(&path, format!("{text}\n").as_bytes())
        .map_err(|_| "cannot write device id")?;
    DeviceId::parse(&text).map_err(|_| "device id is not a UUID".into())
}
