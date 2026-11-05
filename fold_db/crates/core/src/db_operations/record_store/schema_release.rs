//! Resumable release of the exact families minted for a dropped schema.
//!
//! The schema is blocked before this walk, so new writes cannot land behind
//! it. One bounded step reads keys outside the gate, then the format re-reads
//! and releases that selected batch in one native scope. Errors leave the
//! schema installed; the next drop continues over the remaining rows.
//! The format composes record-file, history/queue/count and alternate deletes.
//! Installed alternate revisions cannot change; the current declaration names
//! every revision this schema wrote. A future alternate-revision migration
//! must retain its revision history for this release.

use super::{RangeWalk, RecordStore};
use crate::schema::{Schema, SchemaError};
use crate::storage::format::records::{release_families, ReleaseFamily, ReleaseKind};
use lastdb_model::kind_partition::exact_prefix_bounds;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SchemaRelease {
    pub(crate) records: u64,
    pub(crate) history: u64,
    pub(crate) alternates: u64,
}

impl RecordStore {
    /// Each bounded release opens its own native scope, retaining resumability.
    pub(crate) async fn release_schema_rows(
        &self,
        schema: &Schema,
    ) -> Result<SchemaRelease, SchemaError> {
        let mut out = SchemaRelease::default();
        for family in release_families(schema)? {
            let count = self.release_family(&family).await?;
            match family.kind() {
                ReleaseKind::Records => out.records += count,
                ReleaseKind::History => out.history += count,
                ReleaseKind::Alternates => out.alternates += count,
                ReleaseKind::AppliedDeletes => {}
            }
        }
        Ok(out)
    }

    async fn release_family(&self, family: &ReleaseFamily) -> Result<u64, SchemaError> {
        let (start, end) = exact_prefix_bounds(family.prefix());
        let mut walk = RangeWalk::new(
            start.as_bytes(),
            end.as_bytes(),
            crate::storage::format::SCHEMA_RELEASE_ROWS,
        );
        let mut released = 0;
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            if step.rows.is_empty() {
                continue;
            }
            let keys = step.rows.into_iter().map(|(key, _)| key).collect();
            released += self
                .in_write_scope(self.format.release_schema_batch(keys, family))
                .await?;
        }
        Ok(released)
    }
}
