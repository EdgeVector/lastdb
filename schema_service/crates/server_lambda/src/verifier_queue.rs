//! Release verifier jobs.
//!
//! The registry is not on the install trust path: a node installs a release
//! only when the verifier's release log passes it. The registry's part is to
//! hand the verifier each accepted publish and each owner revocation, as one
//! SQS message on the queue in `RELEASE_VERIFIER_QUEUE_URL`. The verifier checks
//! every signature itself, so a job carries the raw `X-Exemem-Dev-Cert` and
//! `X-Signature` headers and the signed body, not the registry's reading of
//! them.
//!
//! A job that cannot be sent does not fail the request: the release or the
//! revocation is already stored, and the error is logged. The send is
//! awaited before the response goes out, because Lambda can freeze the
//! process as soon as it does.

use schema_service_core::app_release::ReleaseRecord;
use schema_service_server_shared::state::SchemaServiceState;
use serde_json::{json, Value};
use tokio::sync::OnceCell;

const RELEASE_VERIFIER_QUEUE_URL_ENV: &str = "RELEASE_VERIFIER_QUEUE_URL";

/// The SQS client and queue URL, built on first use. `None` when the
/// deployment names no queue.
static QUEUE: OnceCell<Option<(aws_sdk_sqs::Client, String)>> = OnceCell::const_new();

async fn queue() -> Option<&'static (aws_sdk_sqs::Client, String)> {
    QUEUE
        .get_or_init(|| async {
            let url = std::env::var(RELEASE_VERIFIER_QUEUE_URL_ENV)
                .ok()
                .map(|url| url.trim().to_string())
                .filter(|url| !url.is_empty())?;
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            Some((aws_sdk_sqs::Client::new(&config), url))
        })
        .await
        .as_ref()
}

/// `publish` job: one accepted publish (new or repeated) for the verifier to check.
pub(crate) async fn send_publish_job(
    state: &SchemaServiceState,
    record: &ReleaseRecord,
    cert_header: &str,
    sig_header: &str,
    body: &Value,
) {
    send_job(&json!({
        "kind": "publish",
        "env": state.deployment_env_label(),
        "release_id": record.release_id,
        "dev_cert_header": cert_header,
        "signature_header": sig_header,
        "body": body,
    }))
    .await;
}

/// `revoke_request` job: an owner revocation for the verifier to append.
pub(crate) async fn send_revoke_job(
    state: &SchemaServiceState,
    record: &ReleaseRecord,
    cert_header: &str,
    sig_header: &str,
    body: &Value,
) {
    send_job(&json!({
        "kind": "revoke_request",
        "env": state.deployment_env_label(),
        "app_id": record.manifest.app_id,
        "release_id": record.release_id,
        "dev_cert_header": cert_header,
        "signature_header": sig_header,
        "body": body,
    }))
    .await;
}

/// Send one job. Every failure is logged here and goes no further.
async fn send_job(job: &Value) {
    let kind = job.get("kind").and_then(Value::as_str).unwrap_or("-");
    let release_id = job.get("release_id").and_then(Value::as_str).unwrap_or("-");
    let Some((client, queue_url)) = queue().await else {
        tracing::warn!(
            kind,
            release_id,
            "{RELEASE_VERIFIER_QUEUE_URL_ENV} is not set; the verifier job is not sent"
        );
        return;
    };
    match client
        .send_message()
        .queue_url(queue_url)
        .message_body(job.to_string())
        .send()
        .await
    {
        Ok(_) => tracing::info!(kind, release_id, "verifier job sent"),
        Err(e) => tracing::error!(
            kind,
            release_id,
            error = %aws_sdk_sqs::error::DisplayErrorContext(&e),
            "verifier job send failed; the stored record stands"
        ),
    }
}
