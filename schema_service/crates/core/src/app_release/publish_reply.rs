//! HTTP classification for a release publish.

use super::*;

/// The publish both HTTP routers answer.
#[derive(Debug)]
pub enum PublishReleaseReply {
    /// The publish finished. `status` and `body` come from
    /// [`ReleasePublishOutcome::to_http`]. A new release is HTTP 201.
    /// A repeat publish is HTTP 200. `record` is the stored release.
    /// The Lambda router sends the verifier job from `record`.
    /// The other router does not.
    Ready {
        status: u16,
        body: Value,
        record: ReleaseRecord,
    },
    /// The publish failed. `status` and `body` come from
    /// [`ReleaseError::to_http`].
    Failed { status: u16, body: Value },
}

impl SchemaServiceState {
    /// Classify [`Self::publish_release`] for the HTTP routers.
    ///
    /// Each router still checks the path, the body, and the signature
    /// headers. The metric stays inside `publish_release`. The Lambda
    /// router still sends the verifier job after `Ready`.
    #[must_use]
    pub async fn publish_release_reply(
        &self,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> PublishReleaseReply {
        match self
            .publish_release(app_id, cert_header, sig_header, body)
            .await
        {
            Ok(outcome) => {
                let (status, http_body) = outcome.to_http();
                let record = match outcome {
                    ReleasePublishOutcome::Created(record)
                    | ReleasePublishOutcome::Idempotent(record) => record,
                };
                PublishReleaseReply::Ready {
                    status,
                    body: http_body,
                    record,
                }
            }
            Err(error) => {
                let (status, error_body) = error.to_http();
                PublishReleaseReply::Failed {
                    status,
                    body: error_body,
                }
            }
        }
    }
}
