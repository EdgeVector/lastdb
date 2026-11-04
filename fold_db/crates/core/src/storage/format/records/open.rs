//! The key format marker of a node home.
//!
//! A new home holds no reference of any kind, so no reference-completeness
//! proof is needed. The format and account-key proofs
//! authorize the new home.
//! The storage layer opens only the current format. A native home also needs
//! the logical identity marker `typed-ranges` before it can serve. Home
//! creation writes it. A record row is plain JSON, and the block encryption of
//! each run file protects it at rest. Control values have no outer
//! envelope. Startup never rewrites a home.

use super::RecordFormat;
use crate::storage::StorageError;

impl RecordFormat {
    pub(in crate::storage::format) async fn require_current_key_format(
        &self,
    ) -> Result<(), StorageError> {
        let stored = self
            .main_store
            .inner()
            .get(super::super::CURRENT_KEY_FORMAT_KEY.as_bytes())
            .await?;
        if !stored
            .as_deref()
            .is_some_and(lastdb_model::field_key_codec::current_key_format)
        {
            return Err(StorageError::InvalidOperation(
                "the logical key format does not match this node home".into(),
            ));
        }
        let controls = self
            .main_store
            .inner()
            .get_many(vec![
                super::super::CURRENT_KEY_FORMAT_KEY.as_bytes().to_vec(),
                super::super::DELETE_REPLAY_CUTOFF_KEY.as_bytes().to_vec(),
            ])
            .await?;
        if controls.len() != 2 {
            return Err(super::super::control::invalid("incomplete fence read"));
        }
        super::super::control::validate_frozen_replay_floor(
            controls[0].as_deref(),
            controls[1].as_deref(),
        )
        .map(|_| ())
    }
}
