//! Registry snapshot fetch and import calls.

use super::*;

impl SchemaServiceClient {
    pub async fn fetch_snapshot(&self, api_key: Option<&str>) -> FoldDbResult<SnapshotEnvelope> {
        self.fetch_snapshot_at("/v1/snapshot", api_key).await
    }

    /// Fetch the **shared-only** registry projection (`GET /v1/snapshot/shared-only`).
    ///
    /// Use this for resolver-pack publishing. Private legacy bootstrap
    /// schemas are excluded; only system-owned and explicit shared rows
    /// appear.
    pub async fn fetch_shared_only_snapshot(
        &self,
        api_key: Option<&str>,
    ) -> FoldDbResult<SnapshotEnvelope> {
        self.fetch_snapshot_at("/v1/snapshot/shared-only", api_key)
            .await
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

    /// Push a `SnapshotEnvelope` via `POST /v1/snapshot/import`.
    ///
    /// Sled-only on the server side (the Lambda binary does not mount
    /// this route). Useful for tests and for tooling that needs to
    /// seed a local dev binary from a saved JSON file.
    ///
    /// Does not retry: import is a write that mutates server state,
    /// and a partial-success retry could double-clear trees that the
    /// caller already filled in another path.
    pub async fn import_snapshot(
        &self,
        envelope: &SnapshotEnvelope,
    ) -> FoldDbResult<SnapshotImportReport> {
        let url = format!("{}/v1/snapshot/import", self.base_url);
        // trace-egress: propagate (schema_service /v1/snapshot/import)
        let response =
            observability::propagation::inject_w3c(self.client.post(&url).json(envelope))
                .send()
                .await
                .map_err(|e| {
                    FoldDbError::Config(format!("Failed to submit snapshot import to {url}: {e}"))
                })?;
        let status = response.status();
        if status.is_success() {
            response.json::<SnapshotImportReport>().await.map_err(|e| {
                FoldDbError::Config(format!("Failed to parse snapshot import response: {e}"))
            })
        } else {
            let body = response_body_text(response).await;
            Err(FoldDbError::Config(format!(
                "Snapshot import to {url} returned {status}: {body}"
            )))
        }
    }
}
