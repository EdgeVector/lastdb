//! Four request-local tip pages retain their complete captured source coverage.

use super::*;
use crate::db_operations::field_read;
use crate::schema::types::KeyValuePageOrder;

impl CandidatePages {
    /// The discovery batch uses only keys after this round's global cursor.
    /// A retained page certifies absences through its last tip, or to the end
    /// of the selector when exhausted. It creates no warm absence authority.
    pub(super) fn selected_tips(
        &self,
        position: usize,
        keys: &[(String, String)],
    ) -> Option<Vec<CandidateTip>> {
        let page = self.pages.get(position)?.as_ref()?;
        let last = page.tips.last().map(|(key, ..)| key);
        let requested: Vec<_> = keys
            .iter()
            .map(|(hash, range)| KeyValue::new(Some(hash.clone()), Some(range.clone())))
            .collect();
        if requested.iter().any(|key| {
            self.after
                .as_ref()
                .is_some_and(|after| !key.cmp_page_order(after).is_gt())
                || (!page.exhausted && last.is_none_or(|last| key.cmp_page_order(last).is_gt()))
        }) {
            return None;
        }
        let present: HashMap<_, _> = page.tips.iter().map(|tip| (&tip.0, tip)).collect();
        Some(
            requested
                .iter()
                .filter_map(|key| present.get(key).map(|tip| (*tip).clone()))
                .collect(),
        )
    }
}

impl HashRangeQueryProcessor {
    /// Only four source pages survive a refill round. Other sources retain
    /// their bounded reread behavior. Cached and in-flight pages share a fixed
    /// count bound, independent of the number of received namespaces.
    pub(super) async fn received_candidate_source(
        &self,
        field: &FieldVariant,
        filter: &HashRangeFilter,
        after: Option<KeyValue>,
        limit: usize,
        cached: Option<CandidatePage>,
    ) -> Result<(Vec<KeyValue>, bool, Option<CandidatePage>), SchemaError> {
        let Some(mut cached) = cached else {
            let page =
                field_read::hash_range_candidate_page(field, &self.db_ops, filter, after, limit)
                    .await?;
            let keys = page.tips.into_iter().map(|(key, ..)| key).collect();
            return Ok((keys, page.exhausted, None));
        };
        if let Some(after) = &after {
            cached
                .tips
                .retain(|(key, ..)| key.cmp_page_order(after).is_gt());
        }
        while cached.tips.len() < limit && !cached.exhausted {
            let cursor = cached
                .tips
                .last()
                .map(|(key, ..)| key.clone())
                .or_else(|| after.clone());
            let page = field_read::hash_range_candidate_page(
                field,
                &self.db_ops,
                filter,
                cursor.clone(),
                limit - cached.tips.len(),
            )
            .await?;
            if (page.tips.is_empty() && !page.exhausted)
                || page.tips.last().is_some_and(|(key, ..)| {
                    cursor
                        .as_ref()
                        .is_some_and(|cursor| !key.cmp_page_order(cursor).is_gt())
                })
            {
                return Err(SchemaError::InvalidData(
                    "cached received candidate page did not advance".into(),
                ));
            }
            cached.exhausted = page.exhausted;
            cached.tips.extend(page.tips);
        }
        let keys = cached
            .tips
            .iter()
            .take(limit)
            .map(|(key, ..)| key.clone())
            .collect();
        let exhausted = cached.exhausted && cached.tips.len() <= limit;
        Ok((keys, exhausted, Some(cached)))
    }
}
