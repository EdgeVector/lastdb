use super::*;
use futures::StreamExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Deserialize)]
struct ErrorTag<'a> {
    result: Option<&'a str>,
    status: Option<&'a str>,
}

impl BackupTransport {
    pub(in crate::sync::auth) async fn post_bounded<Q: Serialize, R: DeserializeOwned>(
        &self,
        url: Url,
        request: &Q,
        maximum: u64,
    ) -> Result<R> {
        let bytes = cloud_backup_protocol::canonical_bytes(request)?;
        if bytes.len() as u64 > maximum {
            return Err(BackupTransportError::Configuration("request bytes"));
        }
        tokio::time::timeout(self.timeout, self.once(&url, &bytes, maximum))
            .await
            .map_err(|_| BackupTransportError::Timeout)?
    }

    async fn once<R: DeserializeOwned>(&self, url: &Url, bytes: &[u8], maximum: u64) -> Result<R> {
        let request = self
            .http
            .post(url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(bytes.to_vec());
        let request = self.auth.apply_auth(request);
        let response = observability::propagation::inject_w3c(request)
            .send()
            .await
            .map_err(|_| BackupTransportError::Transport)?;
        let status = response.status();
        let body = bounded(response, maximum).await?;
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(BackupTransportError::Authentication);
        }
        // ProviderPending is a safe protocol error with HTTP 202. A successful
        // transport status must not erase the service's explicit classification.
        if let Ok(error) = serde_json::from_slice::<cloud_backup_protocol::ProtocolError>(&body) {
            return Err(BackupTransportError::Protocol(error));
        }
        if !status.is_success() {
            let tagged = serde_json::from_slice::<ErrorTag<'_>>(&body)
                .ok()
                .is_some_and(|tag| tag.result == Some("error") || tag.status == Some("error"));
            if !tagged {
                return Err(BackupTransportError::Http(status.as_u16()));
            }
        }
        serde_json::from_slice(&body)
            .map_err(|_| BackupTransportError::InvalidResponse("JSON body"))
    }
}

async fn bounded(response: reqwest::Response, maximum: u64) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|bytes| bytes > maximum)
    {
        return Err(BackupTransportError::InvalidResponse("response bytes"));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BackupTransportError::Transport)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .filter(|bytes| *bytes as u64 <= maximum)
            .ok_or(BackupTransportError::InvalidResponse("response bytes"))?;
        body.try_reserve_exact(next - body.len())
            .map_err(|_| BackupTransportError::InvalidResponse("response allocation"))?;
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
