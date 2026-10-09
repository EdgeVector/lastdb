//! Quota bucket helpers and the in-memory quota store implementation.

use super::*;

pub(super) fn bucket_len(
    store: &SchemaMutationGateStore,
    cfg: &SchemaMutationGateConfig,
    key: &str,
    now: u64,
) -> Result<usize, SchemaMutationGateError> {
    store.backend.bucket_len(key, cfg.quota_window, now)
}

pub(super) fn check_quota_bucket(
    store: &SchemaMutationGateStore,
    bucket_label: &'static str,
    window_label: &'static str,
    key: &str,
    window: Duration,
    limit: usize,
    now: u64,
) -> Result<(), SchemaMutationGateError> {
    store
        .backend
        .check_quota_bucket(bucket_label, window_label, key, window, limit, now)
}

pub(super) fn prune_bucket(bucket: &mut VecDeque<u64>, window_secs: u64, now: u64) {
    while bucket
        .front()
        .is_some_and(|ts| ts.saturating_add(window_secs) <= now)
    {
        bucket.pop_front();
    }
}

impl SchemaMutationGateQuotaStore for InMemorySchemaMutationGateQuotaStore {
    fn backend_label(&self) -> &'static str {
        "in_memory"
    }

    fn bucket_len(
        &self,
        key: &str,
        window: Duration,
        now: u64,
    ) -> Result<usize, SchemaMutationGateError> {
        let mut quotas = write_lock(&self.quota_events, "schema_mutation_gate.quota_events")
            .map_err(|e| SchemaMutationGateError::Internal(e.to_string()))?;
        let bucket = quotas.entry(key.to_string()).or_default();
        prune_bucket(bucket, window.as_secs(), now);
        Ok(bucket.len())
    }

    fn check_quota_bucket(
        &self,
        bucket_label: &'static str,
        window_label: &'static str,
        key: &str,
        window: Duration,
        limit: usize,
        now: u64,
    ) -> Result<(), SchemaMutationGateError> {
        if limit == 0 {
            return Ok(());
        }
        let mut quotas = write_lock(&self.quota_events, "schema_mutation_gate.quota_events")
            .map_err(|e| SchemaMutationGateError::Internal(e.to_string()))?;
        let bucket = quotas.entry(key.to_string()).or_default();
        prune_bucket(bucket, window.as_secs(), now);
        if bucket.len() >= limit {
            let retry_after_secs = bucket
                .front()
                .copied()
                .map_or(now, |first| first + window.as_secs())
                .saturating_sub(now)
                .max(1);
            return Err(SchemaMutationGateError::QuotaExceeded {
                bucket: bucket_label,
                window: window_label,
                limit,
                retry_after_secs,
            });
        }
        bucket.push_back(now);
        Ok(())
    }
}

pub(super) fn normalize_ip_bucket(ip: &str) -> Option<String> {
    let trimmed = ip.trim();
    if trimmed.is_empty() {
        return None;
    }
    let first = trimmed.split(',').next()?.trim();
    if first.contains(':') {
        let mut parts = first.split(':').take(4).collect::<Vec<_>>();
        while parts.len() < 4 {
            parts.push("0");
        }
        Some(parts.join(":"))
    } else {
        let mut parts = first.split('.').take(3).collect::<Vec<_>>();
        if parts.len() < 3 {
            return Some(first.to_string());
        }
        parts.push("0");
        Some(parts.join("."))
    }
}
