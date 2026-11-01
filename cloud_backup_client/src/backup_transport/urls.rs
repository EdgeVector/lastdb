use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn https(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| BackupTransportError::Configuration("HTTPS URL"))?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(BackupTransportError::Configuration("HTTPS URL"));
    }
    Ok(url)
}

pub(super) fn signed(value: &str, upload: bool) -> Result<Url> {
    signed_headers(
        value,
        if upload {
            "content-length;host;if-none-match"
        } else {
            "host"
        },
    )
}

pub(super) fn upload(value: &str, multipart_part: bool) -> Result<Url> {
    let headers = if multipart_part {
        "content-length;host"
    } else {
        "content-length;host;if-none-match"
    };
    let url = signed_headers(value, headers)?;
    let parts: Vec<_> = url.query_pairs().collect();
    let uploads: Vec<_> = parts.iter().filter(|(key, _)| key == "uploadId").collect();
    let numbers: Vec<_> = parts
        .iter()
        .filter(|(key, _)| key == "partNumber")
        .collect();
    let valid = if multipart_part {
        uploads.len() == 1 && !uploads[0].1.is_empty() && numbers.len() == 1 && numbers[0].1 == "1"
    } else {
        uploads.is_empty() && numbers.is_empty()
    };
    if !valid {
        return Err(BackupTransportError::InvalidResponse("upload operation"));
    }
    drop(parts);
    Ok(url)
}

pub(super) fn signed_headers(value: &str, expected: &str) -> Result<Url> {
    let url = https(value)?;
    let names: Vec<_> = url
        .query_pairs()
        .filter(|(name, _)| name.eq_ignore_ascii_case("x-amz-signedheaders"))
        .map(|(_, value)| value.into_owned())
        .collect();
    if names.len() != 1 || names[0] != expected {
        return Err(BackupTransportError::InvalidResponse("signed headers"));
    }
    Ok(url)
}

impl BackupTransport {
    pub(crate) fn api_endpoint(&self, operation: &str) -> Url {
        let mut url = self.base.clone();
        let base = self.base.path().trim_end_matches('/');
        let api = if base.ends_with("/api") {
            base.to_owned()
        } else {
            format!("{base}/api")
        };
        let path = format!("{api}/{operation}");
        url.set_path(&path);
        url
    }

    pub(super) fn transfer_timeout(&self, bytes: u64, expires_ms: u64) -> Result<Duration> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BackupTransportError::Configuration("clock"))?
            .as_millis();
        let remaining = u128::from(expires_ms)
            .checked_sub(now)
            .filter(|value| *value > 0)
            .ok_or(BackupTransportError::Timeout)?;
        let seconds = u64::try_from(remaining / 1000).map_err(|_| BackupTransportError::Timeout)?;
        if seconds == 0 {
            return Err(BackupTransportError::Timeout);
        }
        let size = usize::try_from(bytes)
            .map_err(|_| BackupTransportError::Configuration("platform size"))?;
        Ok(transfer_budget(self.timeout, size, seconds))
    }
}

// Preserve the established byte-transfer deadline without the legacy S3 client.
fn transfer_budget(request_timeout: Duration, bytes: usize, expires_in_secs: u64) -> Duration {
    let transfer_secs = (bytes as u64).saturating_add(256 * 1024 - 1) / (256 * 1024);
    let slow_uplink_floor = request_timeout.saturating_mul(10);
    let size_budget = request_timeout.saturating_add(Duration::from_secs(transfer_secs));
    let timeout = slow_uplink_floor.max(size_budget);
    let expires_in = Duration::from_secs(expires_in_secs);
    let maximum = expires_in.saturating_sub(Duration::from_secs(30));
    if maximum.is_zero() {
        let remaining = if expires_in.is_zero() {
            Duration::from_secs(1)
        } else {
            expires_in
        };
        timeout.min(remaining)
    } else {
        timeout.min(maximum)
    }
}
