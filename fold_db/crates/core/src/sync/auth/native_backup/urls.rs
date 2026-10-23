use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn https(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| NativeBackupError::Configuration("HTTPS URL"))?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(NativeBackupError::Configuration("HTTPS URL"));
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

pub(super) fn signed_headers(value: &str, expected: &str) -> Result<Url> {
    let url = https(value)?;
    let names: Vec<_> = url
        .query_pairs()
        .filter(|(name, _)| name.eq_ignore_ascii_case("x-amz-signedheaders"))
        .map(|(_, value)| value.into_owned())
        .collect();
    if names.len() != 1 || names[0] != expected {
        return Err(NativeBackupError::InvalidResponse("signed headers"));
    }
    Ok(url)
}

impl NativeBackupClient {
    pub(super) fn endpoint(&self, operation: &str) -> Url {
        self.api_endpoint(&format!("cloud/native-backup/v1/{operation}"))
    }

    pub(super) fn api_endpoint(&self, operation: &str) -> Url {
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
            .map_err(|_| NativeBackupError::Configuration("clock"))?
            .as_millis();
        let remaining = u128::from(expires_ms)
            .checked_sub(now)
            .filter(|value| *value > 0)
            .ok_or(NativeBackupError::Timeout)?;
        let seconds = u64::try_from(remaining / 1000).map_err(|_| NativeBackupError::Timeout)?;
        if seconds == 0 {
            return Err(NativeBackupError::Timeout);
        }
        let size = usize::try_from(bytes)
            .map_err(|_| NativeBackupError::Configuration("platform size"))?;
        Ok(
            crate::sync::s3::S3Client::with_request_timeout(Arc::clone(&self.http), self.timeout)
                .snapshot_transfer_timeout(size, seconds),
        )
    }
}
