//! HTTP classification for a channel write.

use super::*;

/// The channel write both HTTP routers answer.
#[derive(Debug)]
pub enum SetChannelReply {
    /// The channel points at the release. HTTP 200. The value is the public channel.
    Ready(Value),
    /// The write failed. `status` and `body` come from the release error.
    Failed { status: u16, body: Value },
}

impl SchemaServiceState {
    /// Classify [`Self::set_channel`] for the HTTP routers.
    ///
    /// `Ready` is the public channel. `Failed` keeps the status and body from
    /// [`ReleaseError::to_http`]. Each router still checks the path, the body,
    /// and the signature headers. The metric stays inside `set_channel`.
    #[must_use]
    pub async fn set_channel_reply(
        &self,
        app_id: &str,
        channel: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> SetChannelReply {
        match self
            .set_channel(app_id, channel, cert_header, sig_header, body)
            .await
        {
            Ok(record) => SetChannelReply::Ready(record.to_public_json()),
            Err(error) => {
                let (status, error_body) = error.to_http();
                SetChannelReply::Failed {
                    status,
                    body: error_body,
                }
            }
        }
    }
}
