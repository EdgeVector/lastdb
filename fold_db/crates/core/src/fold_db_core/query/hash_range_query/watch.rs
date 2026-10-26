//! Change-feed watch handle for a HashRange query: range bounds, events, errors.

use super::*;

/// Optional range bounds for a HashRange watch.
///
/// The lower bound is inclusive. The upper bound is exclusive, which matches
/// [`HashRangeFilter::HashRangeRange`] and keeps adjacent watches disjoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HashRangeWatchBounds {
    pub start: Option<String>,
    pub end: Option<String>,
}

impl HashRangeWatchBounds {
    #[must_use]
    pub fn new(start: Option<String>, end: Option<String>) -> Self {
        Self { start, end }
    }

    #[must_use]
    pub fn contains(&self, range: &str) -> bool {
        self.start.as_deref().is_none_or(|start| range >= start)
            && self.end.as_deref().is_none_or(|end| range < end)
    }

    fn contains_with(
        &self,
        range: &str,
        comparison: &crate::schema::types::key_comparison::RangeKeyComparison,
    ) -> Result<bool, String> {
        comparison.validate_value(range)?;
        let lower = self
            .start
            .as_deref()
            .map(|start| comparison.compare(range, start))
            .transpose()?;
        let upper = self
            .end
            .as_deref()
            .map(|end| comparison.compare(range, end))
            .transpose()?;
        Ok(lower.is_none_or(|order| !order.is_lt()) && upper.is_none_or(|order| order.is_lt()))
    }
}

/// A committed mutation delivered by a HashRange watch.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HashRangeWatchEvent {
    pub seq: u64,
    pub mutation_id: String,
    pub schema: String,
    pub operation: String,
    pub hash: String,
    pub range: String,
    pub committed_at_ms: u64,
}

impl From<ChangeFeedEvent> for HashRangeWatchEvent {
    fn from(event: ChangeFeedEvent) -> Self {
        Self {
            seq: event.seq,
            mutation_id: event.mutation_id,
            schema: event.schema,
            operation: event.operation,
            hash: event.hash.unwrap_or_default(),
            range: event.range.unwrap_or_default(),
            committed_at_ms: event.committed_at_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashRangeWatchError {
    reason: &'static str,
    skipped: u64,
}

impl HashRangeWatchError {
    #[must_use]
    pub fn skipped(&self) -> u64 {
        self.skipped
    }
}

impl std::fmt::Display for HashRangeWatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.skipped > 0 {
            write!(
                f,
                "HashRange watch {}: skipped {} events",
                self.reason, self.skipped
            )
        } else {
            write!(f, "HashRange watch {}", self.reason)
        }
    }
}

impl std::error::Error for HashRangeWatchError {}

/// A live, schema-and-partition-scoped HashRange mutation stream.
pub struct HashRangeWatch {
    pub(super) schema: String,
    pub(super) hash: String,
    pub(super) bounds: HashRangeWatchBounds,
    pub(super) receiver: broadcast::Receiver<ChangeFeedEvent>,
    pub(super) db_ops: Arc<DbOperations>,
    pub(super) comparison: Option<Arc<crate::schema::types::key_comparison::KeyComparisonContext>>,
}

impl HashRangeWatch {
    pub async fn recv(&mut self) -> Result<HashRangeWatchEvent, HashRangeWatchError> {
        if self.comparison.is_none() {
            let schema = self
                .db_ops
                .schemas()
                .get_schema(&self.schema)
                .await
                .map_err(|_| invalid_watch())?
                .ok_or_else(invalid_watch)?;
            self.comparison = Some(
                self.db_ops
                    .atoms()
                    .key_comparison_context(&schema)
                    .await
                    .map_err(|_| invalid_watch())?,
            );
        }
        let comparison = self
            .comparison
            .as_ref()
            .ok_or_else(invalid_watch)?
            .range_comparison();
        loop {
            let event = match self.receiver.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(HashRangeWatchError {
                        reason: "lagged",
                        skipped,
                    });
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(HashRangeWatchError {
                        reason: "closed",
                        skipped: 0,
                    });
                }
            };
            if event.schema != self.schema
                || event.hash.as_deref() != Some(self.hash.as_str())
                || !matches!(event.operation.as_str(), "create" | "update" | "delete")
            {
                continue;
            }
            let Some(range) = event.range.as_deref() else {
                continue;
            };
            if !self
                .bounds
                .contains_with(range, comparison)
                .map_err(|_| invalid_watch())?
            {
                continue;
            }
            return Ok(HashRangeWatchEvent {
                range: range.to_string(),
                ..event.into()
            });
        }
    }

    pub async fn next(&mut self) -> Result<HashRangeWatchEvent, HashRangeWatchError> {
        self.recv().await
    }
}

fn invalid_watch() -> HashRangeWatchError {
    HashRangeWatchError {
        reason: "invalid_range_contract",
        skipped: 0,
    }
}
