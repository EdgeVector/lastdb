//! Read operations for the v2 release registry.

use super::*;

/// The anonymous compat-row list both HTTP routers answer.
#[derive(Debug)]
pub enum CompatRowsReply {
    /// The list loaded. HTTP 200. The value is `{ "compat_rows": [...] }`.
    Ready(Value),
    /// The storage read failed. Status and body come from [`CompatRowError::to_http`].
    Failed { status: u16, body: Value },
}

impl SchemaServiceState {
    /// `GET /v2/releases/{release_id}/compat` — list all compat rows for a release.
    ///
    /// Reads from storage (not an in-memory cache) because compat rows are
    /// written infrequently by a separate verifier identity, not by the app
    /// publisher.
    ///
    /// # Errors
    /// Returns storage errors.
    pub async fn list_compat_rows(
        &self,
        release_id: &str,
    ) -> Result<Vec<CompatRow>, CompatRowError> {
        self.storage
            .backend()
            .load_compat_rows_for_release(release_id)
            .await
            .map_err(|e| CompatRowError::Internal(e.to_string()))
    }

    /// Classify [`Self::list_compat_rows`] for the anonymous HTTP read.
    ///
    /// `Ready` is the `{ "compat_rows": [...] }` body. `Failed` keeps the
    /// status and body from [`CompatRowError::to_http`].
    #[must_use]
    pub async fn compat_rows_reply(&self, release_id: &str) -> CompatRowsReply {
        match self.list_compat_rows(release_id).await {
            Ok(rows) => {
                let rows_json: Vec<_> = rows.iter().map(CompatRow::to_public_json).collect();
                CompatRowsReply::Ready(json!({ "compat_rows": rows_json }))
            }
            Err(error) => {
                let (status, body) = error.to_http();
                CompatRowsReply::Failed { status, body }
            }
        }
    }

    /// `GET /v2/apps` (anonymous) — list all apps from storage.
    ///
    /// Reads from storage to include legacy apps and new per-record apps.
    /// Ordered by app_id for stable pagination.
    ///
    /// # Errors
    /// Returns storage errors.
    pub async fn list_all_apps(&self) -> Result<Vec<crate::snapshot::AppRecord>, String> {
        let apps = self
            .storage
            .backend()
            .load_all_apps()
            .await
            .map_err(|e| e.to_string())?;
        let mut sorted: Vec<_> = apps.into_values().collect();
        sorted.sort_by(|a, b| a.app_id.cmp(&b.app_id));
        Ok(sorted)
    }

    /// `GET /v2/apps/{app_id}/releases` — list all releases for an app.
    ///
    /// Reads from the in-memory release store and filters by app_id.
    /// Ordered by published_at descending (newest first).
    #[must_use]
    pub fn list_releases_for_app(&self, app_id: &str) -> Vec<ReleaseRecord> {
        let Ok(releases) = read_lock(&self.app_releases, "app_releases") else {
            return Vec::new();
        };
        let mut filtered: Vec<_> = releases
            .values()
            .filter(|r| r.manifest.app_id == app_id)
            .cloned()
            .collect();
        filtered.sort_by(|a, b| b.published_at.cmp(&a.published_at));
        filtered
    }
}
