//! Registry snapshot fetch.

use super::*;

impl SchemaServiceClient {
    pub async fn fetch_snapshot(&self, api_key: Option<&str>) -> FoldDbResult<SnapshotEnvelope> {
        self.fetch_snapshot_at("/v1/snapshot", api_key).await
    }

    pub(super) async fn fetch_snapshot_at(
        &self,
        path: &str,
        api_key: Option<&str>,
    ) -> FoldDbResult<SnapshotEnvelope> {
        let url = format!("{}{path}", self.base_url);
        with_retries(|| async {
            // trace-egress: propagate (schema_service snapshot routes)
            let mut builder = self.client.get(&url);
            if let Some(key) = api_key {
                builder = builder.header("X-API-Key", key);
            }
            let response = observability::propagation::inject_w3c(builder)
                .send()
                .await
                .map_err(|e| {
                    let retryable = reqwest_error_is_retryable(&e);
                    let wrapped =
                        FoldDbError::Config(format!("Failed to fetch snapshot from {url}: {e}"));
                    RetryError::classify(retryable, wrapped)
                })?;
            let status = response.status();
            if !status.is_success() {
                let body = response_body_text(response).await;
                let wrapped = FoldDbError::Config(format!(
                    "Snapshot fetch from {url} returned {status}: {body}"
                ));
                return Err(RetryError::classify(status_is_retryable(status), wrapped));
            }
            response.json::<SnapshotEnvelope>().await.map_err(|e| {
                RetryError::Permanent(FoldDbError::Config(format!(
                    "Failed to parse snapshot envelope: {e}"
                )))
            })
        })
        .await
    }
}
