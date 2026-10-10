use super::*;

use super::attribution::*;

/// Durable response body for `GET /api/storage/home`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeStorageReport {
    pub metric: String,
    pub measured_at: DateTime<Utc>,
    /// Identifier for the filesystem/storage frontier used by reconcile.
    pub snapshot_frontier: Option<String>,
    /// True only when the persisted snapshot passes every additive invariant.
    pub complete: bool,
    /// Always false. The heavy work belongs to the explicit reconcile route.
    pub heavy: bool,
    pub home: HomeStorageTotals,
    /// One row per [`HomeStorageBucketKind`].
    pub unique_physical_buckets: Vec<HomeStorageBucket>,
    /// Bytes in the measured total but absent from the bucket sum.
    pub unaccounted_bytes: u64,
    /// Bytes in the bucket sum beyond the measured total.
    pub overaccounted_bytes: u64,
    /// Apparent-byte equivalents of the allocation residuals.
    pub unaccounted_apparent_bytes: u64,
    pub overaccounted_apparent_bytes: u64,
    /// Accounted bytes in [`HomeStorageBucketKind::UnknownPath`].
    pub unknown_path_bytes: u64,
    /// Named reasons that prevent an exact report.
    #[serde(default)]
    pub unresolved_scopes: Vec<String>,
    /// Logical data counted once per reaching app. This ledger is independent
    /// from the unique physical bucket ledger above and can exceed its own
    /// unique logical total when apps share molecules.
    #[serde(default)]
    pub inclusive_app_attribution: HomeStorageInclusiveAppAttribution,
}

impl HomeStorageReport {
    /// Empty, honest response for a home that has no persisted snapshot yet.
    #[must_use]
    pub fn missing() -> Self {
        Self {
            metric: HOME_STORAGE_METRIC.to_string(),
            measured_at: Utc::now(),
            snapshot_frontier: None,
            complete: false,
            heavy: false,
            home: HomeStorageTotals::default(),
            unique_physical_buckets: Vec::new(),
            unaccounted_bytes: 0,
            overaccounted_bytes: 0,
            unaccounted_apparent_bytes: 0,
            overaccounted_apparent_bytes: 0,
            unknown_path_bytes: 0,
            unresolved_scopes: vec!["snapshot_missing".to_string()],
            inclusive_app_attribution: HomeStorageInclusiveAppAttribution::default(),
        }
    }

    /// Recompute every derived field and fail closed when persisted state is
    /// corrupt or incomplete. This method performs no IO.
    #[must_use]
    pub fn validated_for_read(mut self) -> Self {
        self.metric = HOME_STORAGE_METRIC.to_string();
        self.heavy = false;
        self.inclusive_app_attribution = self.inclusive_app_attribution.validated_for_read();

        let bucket_apparent = self
            .unique_physical_buckets
            .iter()
            .fold(0_u64, |sum, bucket| {
                sum.saturating_add(bucket.apparent_bytes)
            });
        let bucket_allocated = self
            .unique_physical_buckets
            .iter()
            .fold(0_u64, |sum, bucket| {
                sum.saturating_add(bucket.allocated_bytes)
            });
        self.unaccounted_bytes = self.home.allocated_bytes.saturating_sub(bucket_allocated);
        self.overaccounted_bytes = bucket_allocated.saturating_sub(self.home.allocated_bytes);
        self.unaccounted_apparent_bytes = self.home.apparent_bytes.saturating_sub(bucket_apparent);
        self.overaccounted_apparent_bytes =
            bucket_apparent.saturating_sub(self.home.apparent_bytes);
        self.unknown_path_bytes = self
            .unique_physical_buckets
            .iter()
            .filter(|bucket| bucket.kind == HomeStorageBucketKind::UnknownPath)
            .fold(0_u64, |sum, bucket| {
                sum.saturating_add(bucket.allocated_bytes)
            });

        let mut kinds = BTreeSet::new();
        let duplicate_kind = self
            .unique_physical_buckets
            .iter()
            .any(|bucket| !kinds.insert(bucket.kind));
        if duplicate_kind {
            push_scope(&mut self.unresolved_scopes, "duplicate_bucket_kind");
        }
        if self.unaccounted_bytes != 0 || self.overaccounted_bytes != 0 {
            push_scope(&mut self.unresolved_scopes, "allocated_total_mismatch");
        }
        if self.unaccounted_apparent_bytes != 0 || self.overaccounted_apparent_bytes != 0 {
            push_scope(&mut self.unresolved_scopes, "apparent_total_mismatch");
        }
        self.unresolved_scopes.sort_unstable();
        self.unresolved_scopes.dedup();
        self.complete &= self.snapshot_frontier.is_some()
            && self.unresolved_scopes.is_empty()
            && self.unaccounted_bytes == 0
            && self.overaccounted_bytes == 0
            && self.unaccounted_apparent_bytes == 0
            && self.overaccounted_apparent_bytes == 0;
        self
    }

    /// Attach a measured inclusive app ledger to this physical snapshot.
    #[must_use]
    pub fn with_inclusive_app_attribution(
        mut self,
        attribution: HomeStorageInclusiveAppAttribution,
    ) -> Self {
        self.inclusive_app_attribution = attribution.validated_for_read();
        self
    }
}

pub(super) fn push_scope(scopes: &mut Vec<String>, scope: &str) {
    if scopes.iter().any(|existing| existing == scope) {
        return;
    }
    if scopes.len() < UNRESOLVED_SCOPES_MAX.saturating_sub(1) {
        scopes.push(scope.to_string());
    } else if !scopes
        .iter()
        .any(|existing| existing == "additional_unresolved_scopes")
    {
        scopes.push("additional_unresolved_scopes".to_string());
    }
}
