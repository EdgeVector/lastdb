//! Durable state for a schema-root attribution cutover.
//!
//! This module records the attribution protocol only. It never authorizes a
//! delete.
//! A later projector owns the physical walk and advances `applied_frontier`.

use chrono::{DateTime, Utc};
use fold_db::db_operations::MetadataStore;
use fold_db::schema::SchemaError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Metadata key for the active node-wide attribution epoch.
pub const ATTRIBUTION_EPOCH_KEY: &str = "attribution:epoch:v1";
const EPOCH_VERSION: u8 = 1;

/// The only lifecycle states that may persist for an attribution epoch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttributionEpochPhase {
    /// The legacy graph still needs a bounded walk and event replay.
    Backfill,
    /// A read or decode failure made the proof incomplete.
    Blocked,
    /// The projector reached the final frontier with no unknown scope.
    Complete,
}

/// One node-wide attribution proof generation.
///
/// `start_frontier` is H0. The event projector advances
/// `applied_frontier`, then records H1 in `final_frontier`. `Complete` proves
/// that the projector classified the selected snapshot. It does not authorize
/// deletion. A later target-specific reclaim proof owns that decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributionEpoch {
    pub version: u8,
    pub epoch_id: String,
    pub started_at: DateTime<Utc>,
    /// Identity of the source home or source snapshot that the walker classifies.
    pub source_snapshot_id: String,
    pub catalog_generation: String,
    pub start_frontier: u64,
    pub applied_frontier: u64,
    pub final_frontier: Option<u64>,
    /// Exact isolated-copy snapshot identity. It stays absent until the
    /// projector reaches a stable source frontier and the snapshot gate holds.
    #[serde(default)]
    pub copy_snapshot_id: Option<String>,
    /// Source-event frontier captured with [`Self::copy_snapshot_id`]. The
    /// copy can only prove the data state at this exact frontier.
    #[serde(default)]
    pub copy_frontier: Option<u64>,
    /// The walker sets this only after every configured object cursor ends.
    pub walk_complete: bool,
    pub phase: AttributionEpochPhase,
    #[serde(default)]
    pub unknown_scopes: BTreeSet<String>,
}

impl AttributionEpoch {
    /// Start a backfill. `start_frontier` must come from the durable attribution
    /// event source, not a wall clock.
    pub fn start(
        now: DateTime<Utc>,
        source_snapshot_id: impl Into<String>,
        catalog_generation: impl Into<String>,
        start_frontier: u64,
    ) -> Result<Self, String> {
        let source_snapshot_id = source_snapshot_id.into();
        if source_snapshot_id.trim().is_empty() {
            return Err("attribution epoch requires a source snapshot identity".to_string());
        }
        let catalog_generation = catalog_generation.into();
        if catalog_generation.trim().is_empty() {
            return Err("attribution epoch requires a catalog generation".to_string());
        }
        Ok(Self {
            version: EPOCH_VERSION,
            epoch_id: format!("attribution-{}-{start_frontier}", now.timestamp_micros()),
            started_at: now,
            source_snapshot_id,
            catalog_generation,
            start_frontier,
            applied_frontier: start_frontier,
            final_frontier: None,
            copy_snapshot_id: None,
            copy_frontier: None,
            walk_complete: false,
            phase: AttributionEpochPhase::Backfill,
            unknown_scopes: BTreeSet::new(),
        })
    }

    /// Record H1 after the write path makes attribution events durable before ACK.
    ///
    /// This closes classification. It does not permit deletion.
    pub fn complete(&mut self, final_frontier: u64) -> Result<(), String> {
        if !self.walk_complete {
            return Err("attribution object walk is incomplete".to_string());
        }
        if final_frontier < self.start_frontier {
            return Err(format!(
                "final attribution frontier {final_frontier} precedes start frontier {}",
                self.start_frontier
            ));
        }
        if self.applied_frontier < final_frontier {
            return Err(format!(
                "attribution projector reached {}, below final frontier {final_frontier}",
                self.applied_frontier
            ));
        }
        if self.copy_snapshot_id.is_none() || self.copy_frontier != Some(final_frontier) {
            return Err(
                "attribution epoch needs an exact copy snapshot at the final frontier".to_string(),
            );
        }
        if !self.unknown_scopes.is_empty() {
            return Err("attribution epoch has unknown scopes".to_string());
        }
        self.final_frontier = Some(final_frontier);
        self.phase = AttributionEpochPhase::Complete;
        Ok(())
    }

    /// Load the sole active node attribution epoch by exact metadata key.
    pub async fn load(metadata: &MetadataStore) -> Result<Option<Self>, SchemaError> {
        metadata.get_typed(ATTRIBUTION_EPOCH_KEY).await
    }

    /// Persist the complete checkpoint before a caller returns a cursor or a
    /// successful state transition.
    pub async fn persist(&self, metadata: &MetadataStore) -> Result<(), SchemaError> {
        metadata
            .put_typed_durable(ATTRIBUTION_EPOCH_KEY, self)
            .await
    }
}
