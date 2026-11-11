//! Capability negotiation for a publication with an accepted mutation tail.
use super::{BackupTransportError, ByteBackupClient, Result};
use cloud_backup_protocol::v2::Stream;
use cloud_backup_protocol::ErrorCode;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct Request<'a> {
    stream: &'a Stream,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Status {
    stream: Stream,
    max_log_checkpoints: u8,
}

impl ByteBackupClient {
    /// Return the retained checkpoint bound when guest recovery is enabled.
    pub async fn share_recovery_limit(&self, stream: &Stream) -> Result<Option<u8>> {
        stream.validate()?;
        let response: Status = match self
            .post("streams/share-recovery/status", &Request { stream })
            .await
        {
            Ok(response) => response,
            Err(BackupTransportError::Http(404)) => return Ok(None),
            Err(BackupTransportError::Protocol(error))
                if (error.code == ErrorCode::NotFound && error.field == "http.route")
                    || (error.code == ErrorCode::UnsupportedCapability
                        && error.field == "guest.disabled") =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if &response.stream != stream || !(1..=8).contains(&response.max_log_checkpoints) {
            return Err(BackupTransportError::InvalidResponse(
                "share recovery capability",
            ));
        }
        Ok(Some(response.max_log_checkpoints))
    }
}
