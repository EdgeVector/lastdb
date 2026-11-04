//! Moving retention state when a manifest name points at a new anchor.
//!
//! A re-declare can register a new catalog schema for a manifest name (the app
//! adds a field). The policy, including an operator override, and the tracking
//! that finds expired records (the HashRange partition registry and the Hash
//! and Single age index) must follow the schema. A policy left on the old
//! anchor would keep sweeping a schema that no longer holds the data, and the
//! new anchor would run a policy of its own.

use super::catalog_keys::*;
use super::{SchemaStore, StateChange, StateDelete, StateKey, Tracking};
use crate::schema::SchemaError;

/// Rows moved for each storage round trip.
const MOVE_PAGE_ROWS: usize = 256;

impl SchemaStore {
    /// Make the policy follow a manifest name to the catalog schema it is bound
    /// to now. Returns the anchor it moved from, or `None` when the name has no
    /// alias or already points at `anchor`.
    ///
    /// The policy then governs the new catalog schema, so the same guard as a
    /// new manifest TTL applies: it refuses when another app owns the schema or
    /// another schema is bound to it. Nothing moves in that case.
    pub async fn realign_retention_anchor(
        &self,
        binding: &str,
        anchor: &str,
        owner_app: &str,
    ) -> Result<Option<String>, SchemaError> {
        let current = self.retention_anchor(binding).await?;
        if current == binding || current == anchor {
            return Ok(None);
        }
        if self.read_retention_policy(&current).await?.is_some() {
            let sharing = self
                .retention_sharing(binding, anchor, owner_app, &[])
                .await?;
            if let Some(reason) = sharing.refusal(binding, anchor, owner_app) {
                return Err(SchemaError::InvalidData(format!(
                    "the retention policy cannot follow {binding} from {current} to {anchor}: \
                     {reason}"
                )));
            }
        }
        self.move_retention_anchor(binding, &current, anchor)
            .await?;
        Ok(Some(current))
    }

    /// Move the retention state of `from` to `to` and point `binding` at `to`.
    ///
    /// Refuses when another manifest name still points at `from`, or when `to`
    /// already holds a different policy. Idempotent and resumable. The order is:
    /// copy the policy and the tracking rows, delete the old policy, then point
    /// the alias at `to`. An interruption at any step leaves the alias on
    /// `from`, and the next call finishes the move.
    pub(super) async fn move_retention_anchor(
        &self,
        binding: &str,
        from: &str,
        to: &str,
    ) -> Result<(), SchemaError> {
        let others: Vec<String> = self
            .retention_aliases_of(from)
            .await?
            .into_iter()
            .filter(|name| name != binding)
            .collect();
        if !others.is_empty() {
            return Err(SchemaError::InvalidData(format!(
                "the retention policy on {from} is still used by [{}], so it cannot follow \
                 {binding} to {to}",
                others.join(", ")
            )));
        }
        if let Some(carried) = self.read_retention_policy(from).await? {
            match self.read_retention_policy(to).await? {
                Some(held) if held != carried => {
                    return Err(SchemaError::InvalidData(format!(
                        "{from} and {to} hold different retention policies. Clear one with \
                         `lastdb schema-retention clear`, then declare the manifest again"
                    )));
                }
                Some(_) => {}
                None => {
                    self.format
                        .put_state(StateChange::Policy {
                            schema: to,
                            policy: &carried,
                        })
                        .await?;
                }
            }
            self.move_tracking_rows(from, to).await?;
            // The old policy goes BEFORE the alias moves. A crash or a failed
            // delete leaves the alias on `from`, so a retry still sees a move
            // to finish; with the alias on `to` first, a retry would return
            // early and the old policy would keep sweeping shared records.
            self.format.delete_state(from, StateKey::Policy).await?;
            self.format.flush_states().await?;
        }
        let row = SchemaRetentionAnchor {
            anchor: to.to_string(),
        };
        self.format
            .put_state(StateChange::Anchor {
                schema: binding,
                anchor: &row,
            })
            .await?;
        // A write that registered under `from` while the rows moved.
        self.move_tracking_rows(from, to).await?;
        self.format.flush_states().await?;
        tracing::info!(
            binding = %binding,
            from = %from,
            to = %to,
            "moved the retention state to the new catalog schema"
        );
        Ok(())
    }

    /// Re-key the HashRange partition registry and the age index of `from`
    /// under `to`, in pages, deleting each page after its copy is written.
    async fn move_tracking_rows(&self, from: &str, to: &str) -> Result<(), SchemaError> {
        let store = &self.schema_states_store;
        loop {
            let page = store
                .scan_items_with_prefix_paged::<SchemaRetentionHashPartition>(
                    &retention_hash_partition_prefix(from),
                    MOVE_PAGE_ROWS,
                )
                .await?;
            if page.is_empty() {
                break;
            }
            let copies = page
                .iter()
                .map(|(_, row)| StateChange::Partition {
                    schema: to,
                    entry: row.clone(),
                })
                .collect();
            self.format.batch_states(copies).await?;
            self.format
                .delete_states(vec![StateDelete::Selected {
                    schema: from,
                    kind: Tracking::Partition,
                    keys: page.into_iter().map(|(key, _)| key).collect(),
                }])
                .await?;
        }
        for latest in [false, true] {
            let prefix = if latest {
                retention_age_latest_prefix(from)
            } else {
                retention_age_prefix(from)
            };
            loop {
                let page = store
                    .scan_items_with_prefix_paged::<SchemaRetentionAgeEntry>(
                        &prefix,
                        MOVE_PAGE_ROWS,
                    )
                    .await?;
                if page.is_empty() {
                    break;
                }
                let copies = page
                    .iter()
                    .map(|(_, entry)| StateChange::Age {
                        schema: to,
                        entry: entry.clone(),
                        latest,
                    })
                    .collect();
                self.format.batch_states(copies).await?;
                self.format
                    .delete_states(vec![StateDelete::Selected {
                        schema: from,
                        kind: if latest {
                            Tracking::Latest
                        } else {
                            Tracking::Age
                        },
                        keys: page.into_iter().map(|(key, _)| key).collect(),
                    }])
                    .await?;
            }
        }
        Ok(())
    }
}
