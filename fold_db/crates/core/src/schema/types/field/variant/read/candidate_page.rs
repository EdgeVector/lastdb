//! Bounded candidate keys precede namespace precedence and layout membership.

use super::*;

/// A raw tip batch does not certify that any candidate has a visible atom.
pub(crate) struct HashRangeCandidatePage {
    pub(crate) keys: Vec<KeyValue>,
    pub(crate) exhausted: bool,
}

impl FieldVariant {
    /// The caller holds the field binding and one captured read scope across
    /// candidate discovery, exact-key namespace resolution, and membership.
    /// Metadata tombstones stay in this batch; the exact resolver decides
    /// their visibility without allowing hydration to hide the key frontier.
    pub(crate) async fn hash_range_candidate_page(
        &self,
        db_ops: &Arc<DbOperations>,
        filter: &HashRangeFilter,
        after: Option<KeyValue>,
        limit: usize,
    ) -> Result<HashRangeCandidatePage, SchemaError> {
        self.require_supported_read(Some(filter))?;
        if self.kind != FieldKind::HashRange || !(1..=1024).contains(&limit) {
            return Err(SchemaError::InvalidData(
                "candidate page requires a HashRange field and 1..=1024 keys".into(),
            ));
        }
        let (HashRangeFilter::HashKey(hash)
        | HashRangeFilter::HashRangePrefix { hash, .. }
        | HashRangeFilter::HashRangeRange { hash, .. }) = filter
        else {
            return Err(SchemaError::InvalidData(
                "candidate page requires one hash selector".into(),
            ));
        };
        let Some(molecule) = self.common().molecule_uuid() else {
            return Ok(HashRangeCandidatePage {
                keys: Vec::new(),
                exhausted: true,
            });
        };
        let window = after.map_or_else(
            || KeyWindow::offset(0, limit),
            |after| KeyWindow::after(after, limit),
        );
        let rows = db_ops
            .atoms()
            .scan_hash_tip_window(
                molecule,
                filter,
                self.common().storage_prefix(),
                &window,
                true,
            )
            .await?
            .ok_or_else(|| {
                SchemaError::InvalidData("candidate page has no bounded reader".into())
            })?;
        // A full final batch needs one empty follow-up. Deduplication must not
        // turn a full physical batch into a false exhaustion claim.
        let exhausted = rows.len() < limit;
        let mut keys: Vec<_> = Self::api_rows_from_scan(db_ops, hash, rows)?
            .into_iter()
            .map(|(key, ..)| key)
            .collect();
        keys.sort_by(KeyValue::cmp_page_order);
        keys.dedup();
        Ok(HashRangeCandidatePage { keys, exhausted })
    }
}
