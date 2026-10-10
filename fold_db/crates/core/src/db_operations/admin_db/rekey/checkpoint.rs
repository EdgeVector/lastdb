//! Durable checkpoint persistence for the rekey pass.

use super::*;

impl AtomStore {
    /// Merge this call's counters onto the durable baseline and persist.
    ///
    /// Called after every page as well as at the end of a pass, so it must be
    /// idempotent in the number of times it runs: it re-derives the checkpoint
    /// from `baseline + report` rather than incrementing in place.
    pub(in crate::db_operations::admin_db) async fn persist_rekey_checkpoint(
        &self,
        checkpoint_key: &str,
        baseline: &AtomPartitionRekeyCheckpoint,
        report: &AtomPartitionRekeyReport,
        last_tip: &Option<String>,
        completed: bool,
    ) -> Result<AtomPartitionRekeyCheckpoint, SchemaError> {
        let checkpoint = AtomPartitionRekeyCheckpoint {
            version: 1,
            after_tip_key: last_tip.clone().or_else(|| baseline.after_tip_key.clone()),
            tips_scanned: report.tips_scanned,
            tips_walked_total: baseline
                .tips_walked_total
                .saturating_add(report.tips_scanned),
            dual_written: baseline.dual_written.saturating_add(report.dual_written),
            already_prefixed: baseline
                .already_prefixed
                .saturating_add(report.already_prefixed),
            missing_body: baseline.missing_body.saturating_add(report.missing_body),
            flat_removed: baseline.flat_removed.saturating_add(report.flat_removed),
            completed,
        };
        self.raw()
            .put_item(
                checkpoint_key,
                &serde_json::to_value(&checkpoint).map_err(|e| {
                    SchemaError::InvalidData(format!("rekey encode checkpoint: {e}"))
                })?,
            )
            .await
            .map_err(|e| SchemaError::InvalidData(format!("rekey put checkpoint: {e}")))?;
        Ok(checkpoint)
    }

    pub(in crate::db_operations::admin_db) async fn load_atom_partition_rekey_checkpoint(
        &self,
        checkpoint_key: &str,
    ) -> Result<AtomPartitionRekeyCheckpoint, SchemaError> {
        let raw = self
            .raw()
            .get_item::<Value>(checkpoint_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("rekey load checkpoint: {e}")))?;
        match raw {
            None => Ok(AtomPartitionRekeyCheckpoint {
                version: 1,
                ..Default::default()
            }),
            Some(v) => {
                // A checkpoint written before `tips_walked_total` existed has
                // no such key, and `#[serde(default)]` cannot tell that absence
                // from a stored `0`. Decide it here, where the raw object is
                // still in hand, so a legitimately-zero fresh checkpoint is
                // left alone and only the legacy shape is seeded.
                //
                // The seed matters beyond one reading: every later pass
                // accumulates onto this baseline (see
                // `persist_rekey_checkpoint`), so a zero inherited here is
                // inherited for the rest of the migration's life. That is how a
                // resumed job reported ~1% walked with ~10% done. `tips_scanned`
                // is the right source: under the old un-paginated walk it
                // tracked the cursor's position, which is what the numerator
                // means.
                let had_walked_total = v.get("tips_walked_total").is_some();
                let mut checkpoint: AtomPartitionRekeyCheckpoint = serde_json::from_value(v)
                    .map_err(|e| {
                        SchemaError::InvalidData(format!("rekey corrupt checkpoint: {e}"))
                    })?;
                if !had_walked_total {
                    checkpoint.tips_walked_total = checkpoint.tips_scanned;
                }
                Ok(checkpoint)
            }
        }
    }
}
