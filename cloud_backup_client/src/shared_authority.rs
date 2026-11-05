use crate::backup_transport::BackupTransportError;
use cloud_backup_protocol::{DecimalU64, Id};
use reqwest::header::{HeaderMap, HeaderValue};

pub(crate) fn device_headers(
    device: &Id,
    expires_at_ms: DecimalU64,
    proof: [u8; 64],
) -> Result<HeaderMap, BackupTransportError> {
    let mut signature = header(&lastdb_model::hex::hex_lower(proof))?;
    signature.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert("x-lastdb-device", header(device.as_str())?);
    headers.insert(
        "x-lastdb-device-expires-ms",
        header(&expires_at_ms.get().to_string())?,
    );
    headers.insert("x-lastdb-device-signature", signature);
    Ok(headers)
}

fn header(value: &str) -> Result<HeaderValue, BackupTransportError> {
    HeaderValue::from_bytes(value.as_bytes())
        .map_err(|_| BackupTransportError::Configuration("shared device proof"))
}
