use super::{http::bounded, BackupTransport, BackupTransportError, Result};
use crate::shared_log::AppendOutcome;
use cloud_backup_protocol::shared::{BatchReceipt, LogBatch, LogHead, APPEND_PATH};
use cloud_backup_protocol::{canonical_bytes, limits, ProtocolError};
use reqwest::{header, StatusCode};

impl BackupTransport {
    pub(crate) async fn post_shared_append(
        &self,
        batch: &LogBatch,
        headers: header::HeaderMap,
    ) -> Result<AppendOutcome> {
        let bytes = canonical_bytes(batch)?;
        if bytes.len() as u64 > limits::REQUEST_JSON_MAX_BYTES {
            return Err(BackupTransportError::Configuration("request bytes"));
        }
        let request = self
            .http
            .post(self.api_endpoint(APPEND_PATH.trim_start_matches('/')))
            .headers(headers)
            .header(header::CONTENT_TYPE, "application/json")
            .body(bytes);
        let request = self.apply_auth(request);
        tokio::time::timeout(self.timeout, async {
            let response = observability::propagation::inject_w3c(request)
                .send()
                .await
                .map_err(|_| BackupTransportError::Transport)?;
            read_reply(response, batch).await
        })
        .await
        .map_err(|_| BackupTransportError::Timeout)?
    }
}

async fn read_reply(response: reqwest::Response, batch: &LogBatch) -> Result<AppendOutcome> {
    let status = response.status();
    let reconcile = status == StatusCode::CONFLICT
        && response
            .headers()
            .get("x-lastdb-reconcile")
            .is_some_and(|value| value == "writer-head");
    let body = bounded(response, limits::REQUEST_JSON_MAX_BYTES).await?;
    if status == StatusCode::UNAUTHORIZED {
        return Err(BackupTransportError::Authentication);
    }
    if let Ok(error) = serde_json::from_slice::<ProtocolError>(&body) {
        return Err(BackupTransportError::Protocol(error));
    }
    if reconcile {
        let head: LogHead = serde_json::from_slice(&body)
            .map_err(|_| BackupTransportError::InvalidResponse("shared writer head"))?;
        head.validate()?;
        if head.sequence < batch.through {
            return Err(BackupTransportError::Integrity("shared writer head"));
        }
        return Ok(AppendOutcome::Reconcile(head));
    }
    if status != StatusCode::OK {
        return Err(BackupTransportError::Http(status.as_u16()));
    }
    let receipt: BatchReceipt = serde_json::from_slice(&body)
        .map_err(|_| BackupTransportError::InvalidResponse("shared batch receipt"))?;
    receipt
        .validate_for(batch)
        .map_err(|_| BackupTransportError::Integrity("shared batch receipt"))?;
    Ok(AppendOutcome::Accepted(receipt))
}
