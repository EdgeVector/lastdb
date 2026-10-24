//! Bounded candidate keys precede namespace precedence and layout membership.

use super::*;

/// A raw tip batch does not certify that any candidate has a visible atom.
pub(crate) struct HashRangeCandidatePage {
    pub(crate) tips: Vec<(
        KeyValue,
        crate::db_operations::atom_store::PerKeyRecord,
        String,
    )>,
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
                tips: Vec::new(),
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
        let mut tips = Self::api_rows_from_scan(db_ops, hash, rows)?;
        tips.sort_by(|(left, ..), (right, ..)| left.cmp_page_order(right));
        tips.dedup_by(|(left, ..), (right, ..)| left == right);
        Ok(HashRangeCandidatePage { tips, exhausted })
    }

    /// Reuse tips already selected by this captured candidate walk. Absences
    /// are certified by the caller's retained page coverage, not admitted as
    /// a new global warm-set authority. Body and metadata visibility remain
    /// identical to an ordinary selected-tip field read.
    pub(crate) async fn materialize_hash_range_candidates(
        &self,
        db_ops: &Arc<DbOperations>,
        tips: Vec<(
            KeyValue,
            crate::db_operations::atom_store::PerKeyRecord,
            String,
        )>,
        include_tombstones: bool,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        let Some(molecule) = self.common().molecule_uuid() else {
            return Ok(HashMap::new());
        };
        let visible = Self::page_visible_matches(tips, None, true, include_tombstones);
        let mut values = Self::materialize_tip_matches(
            db_ops,
            molecule,
            self.common().storage_prefix(),
            include_tombstones,
            visible,
        )
        .await?;
        self.stamp_molecule_meta(&mut values);
        self.record_resolved_tip_keys(&values);
        Ok(values)
    }
}
