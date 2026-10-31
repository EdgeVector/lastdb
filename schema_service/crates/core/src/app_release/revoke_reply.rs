//! HTTP classification for a release revocation.

use super::*;

/// The revocation both HTTP routers answer.
#[derive(Debug)]
pub enum RevokeReleaseReply {
    /// The revocation finished. Success is HTTP 200. `body` is
    /// [`ReleaseRecord::to_public_json`]. `record` is the stored release.
    /// The box keeps this variant near the failure variant in size.
    /// The Lambda router sends the verifier job from `record`.
    /// The other router does not.
    Ready {
        status: u16,
        body: Value,
        record: Box<ReleaseRecord>,
    },
    /// The revocation failed. `status` and `body` come from
    /// [`ReleaseError::to_http`].
    Failed { status: u16, body: Value },
}

impl SchemaServiceState {
    /// Classify [`Self::revoke_release`] for the HTTP routers.
    ///
    /// Each router still checks the path, the body, and the signature
    /// headers. The metric stays inside `revoke_release`. The Lambda
    /// router still sends the verifier job after `Ready`.
    #[must_use]
    pub async fn revoke_release_reply(
        &self,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> RevokeReleaseReply {
        match self
            .revoke_release(app_id, cert_header, sig_header, body)
            .await
        {
            Ok(record) => {
                let http_body = record.to_public_json();
                RevokeReleaseReply::Ready {
                    status: 200,
                    body: http_body,
                    record: Box::new(record),
                }
            }
            Err(error) => {
                let (status, error_body) = error.to_http();
                RevokeReleaseReply::Failed {
                    status,
                    body: error_body,
                }
            }
        }
    }
}
