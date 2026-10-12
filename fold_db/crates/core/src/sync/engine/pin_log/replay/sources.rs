//! Exact source lookup after normal ciphertext and group validation.

use super::super::*;

pub(super) struct ReplaySources<'a> {
    enabled: bool,
    objects: BTreeMap<String, &'a MutationLogSegment>,
}

impl<'a> ReplaySources<'a> {
    pub(super) fn new(segments: &'a [MutationLogSegment], enabled: bool) -> SyncResult<Self> {
        let mut objects = BTreeMap::<String, &MutationLogSegment>::new();
        if enabled {
            for object in segments {
                if let Some(previous) = objects.get(&object.segment.object_key) {
                    if previous.segment != object.segment || previous.payload != object.payload {
                        return Err(SyncError::Storage(
                            "repeated authenticated object has different bytes".into(),
                        ));
                    }
                } else {
                    objects.insert(object.segment.object_key.clone(), object);
                }
            }
        }
        Ok(Self { enabled, objects })
    }

    pub(super) fn source_for(
        &self,
        record: &PinLogRecord,
        record_index: usize,
        transaction_group: bool,
        keys: &[String],
    ) -> SyncResult<Option<RetiredReplaySource>> {
        if !self.enabled
            || !record.target_prefix.is_empty()
            || !matches!(
                record.entry.op,
                crate::sync::log::LogOp::MutationIntent { .. }
            )
        {
            return Ok(None);
        }
        let objects = keys
            .iter()
            .map(|key| {
                self.objects
                    .get(key)
                    .map(|object| AuthenticatedReplayObject::from(*object))
                    .ok_or_else(|| {
                        SyncError::Storage(
                            "authenticated source object is missing from the validated batch"
                                .into(),
                        )
                    })
            })
            .collect::<SyncResult<Vec<_>>>()?;
        Ok(Some(RetiredReplaySource {
            record: record.clone(),
            record_index,
            transaction_group,
            objects,
        }))
    }
}
