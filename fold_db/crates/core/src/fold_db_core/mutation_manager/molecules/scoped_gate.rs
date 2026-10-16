//! Scoped identities stay in their exact durable lane until resident scope exists.

use super::apply_gate::PurgeApplyOutcome;
use crate::schema::types::Schema;

pub(super) fn schema_is_scoped(schema: &Schema) -> bool {
    schema
        .runtime_fields
        .values()
        .any(|field| field.common().storage_prefix().is_some())
}

impl PurgeApplyOutcome {
    pub(super) fn scoped() -> Self {
        Self {
            schema_tombstones: Vec::new(),
            slots: Vec::new(),
            slot_revisions: Vec::new(),
        }
    }
}
