use super::*;
use cloud_backup_protocol::limits::REQUEST_JSON_MAX_BYTES;
use futures::StreamExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Deserialize)]
struct ErrorTag<'a> {
    result: Option<&'a str>,
    status: Option<&'a str>,
}

impl NativeBackupClient {
    pub(super) async fn post<Q: Serialize, R: DeserializeOwned>(
        &self,
        operation: &str,
        request: &Q,
    ) -> Result<R> {
        self.post_url(self.endpoint(operation), request).await
    }

    pub(super) async fn post_url<Q: Serialize, R: DeserializeOwned>(
        &self,
        url: Url,
        request: &Q,
    ) -> Result<R> {
        let bytes = cloud_backup_protocol::canonical_bytes(request)?;
        if bytes.len() as u64 > REQUEST_JSON_MAX_BYTES {
            return Err(NativeBackupError::Configuration("request bytes"));
        }
        let mut refreshes = 0;
        loop {
            let result = tokio::time::timeout(self.timeout, self.once(&url, &bytes))
                .await
                .map_err(|_| NativeBackupError::Timeout)?;
            if !matches!(result, Err(NativeBackupError::Authentication))
                || refreshes == super::super::MAX_AUTH_REFRESH_RETRIES
            {
                return result;
            }
            let Some(refresh) = &self.auth.auth_refresh else {
                return result;
            };
            let auth = tokio::time::timeout(self.timeout, refresh())
                .await
                .map_err(|_| NativeBackupError::Timeout)?
                .map_err(|_| NativeBackupError::Authentication)?;
            self.auth.update_auth(auth).await;
            refreshes += 1;
        }
    }

    async fn once<R: DeserializeOwned>(&self, url: &Url, bytes: &[u8]) -> Result<R> {
        let request = self
            .http
            .post(url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(bytes.to_vec());
        let request = self.auth.apply_auth(request).await;
        let response = observability::propagation::inject_w3c(request)
            .send()
            .await
            .map_err(|_| NativeBackupError::Transport)?;
        let status = response.status();
        let body = bounded(response).await?;
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(NativeBackupError::Authentication);
        }
        // ProviderPending is a safe protocol error with HTTP 202. A successful
        // transport status must not erase the service's explicit classification.
        if let Ok(error) = serde_json::from_slice::<cloud_backup_protocol::ProtocolError>(&body) {
            return Err(NativeBackupError::Protocol(error));
        }
        if !status.is_success() {
            let tagged = serde_json::from_slice::<ErrorTag<'_>>(&body)
                .ok()
                .is_some_and(|tag| tag.result == Some("error") || tag.status == Some("error"));
            if !tagged {
                return Err(NativeBackupError::Http(status.as_u16()));
            }
        }
        serde_json::from_slice(&body).map_err(|_| NativeBackupError::InvalidResponse("JSON body"))
    }
}

async fn bounded(response: reqwest::Response) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|bytes| bytes > REQUEST_JSON_MAX_BYTES)
    {
        return Err(NativeBackupError::InvalidResponse("response bytes"));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| NativeBackupError::Transport)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .filter(|bytes| *bytes as u64 <= REQUEST_JSON_MAX_BYTES)
            .ok_or(NativeBackupError::InvalidResponse("response bytes"))?;
        body.try_reserve_exact(next - body.len())
            .map_err(|_| NativeBackupError::InvalidResponse("response allocation"))?;
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
