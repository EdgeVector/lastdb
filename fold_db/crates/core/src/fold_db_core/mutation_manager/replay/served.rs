//! Put back the preimage of a local write that a served entry precedes.
//!
//! A personal database picks a winner by stamp. A shared mutation log does not.
//! The service assigned one order. This module does not compare stamps. It only
//! removes a local write from the record the next served entry is judged against,
//! or clears the marker when that entry is the local write itself.

use super::records::as_mutation;
use super::state::decode;
use super::MutationManager;
use crate::schema::types::KeyValue;
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction;
use std::sync::Arc;

/// What a served entry found in front of the record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServedLocal {
    /// Nothing local was waiting. The record is whatever earlier entries left.
    Clear,
    /// This entry is the local write. The record already has it.
    Echo,
    /// A different local write was replaced by its preimage.
    Restored,
}

impl MutationManager {
    /// Make the live record the one earlier served entries left.
    ///
    /// The same mutation id clears the local marker and leaves the record.
    /// A different pending id restores the stored preimage and clears that marker.
    pub async fn release_local_for_served(
        &self,
        schema: &str,
        key: &KeyValue,
        served_id: &str,
    ) -> Result<ServedLocal, SchemaError> {
        let runtime = Arc::clone(&self.db_ops.native_runtime());
        transaction::scope(runtime, async {
            match self.pending_replay_record(schema, key).await?.as_deref() {
                Some(id) if id == served_id => {
                    self.clear_pending_replay_record(schema, key, served_id)
                        .await?;
                    Ok(ServedLocal::Echo)
                }
                Some(_) => {
                    self.restore_preimage(schema, key).await?;
                    Ok(ServedLocal::Restored)
                }
                None => Ok(ServedLocal::Clear),
            }
        })
        .await
    }

    /// Remove one local write the service refused, when it is still the pending write.
    ///
    /// `Ok(false)` means that mutation is not the pending one.
    pub async fn undo_refused_local(
        &self,
        schema: &str,
        key: &KeyValue,
        mutation_id: &str,
    ) -> Result<bool, SchemaError> {
        let runtime = Arc::clone(&self.db_ops.native_runtime());
        transaction::scope(runtime, async {
            if self.pending_replay_record(schema, key).await?.as_deref() != Some(mutation_id) {
                return Ok(false);
            }
            self.restore_preimage(schema, key).await?;
            Ok(true)
        })
        .await
    }

    async fn restore_preimage(&self, schema: &str, key: &KeyValue) -> Result<(), SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        let capture = self.replay_capture(&address).await?;
        let bytes = capture.baseline.ok_or_else(|| {
            SchemaError::InvalidData("replay baseline missing for a pending local write".into())
        })?;
        let baseline = decode(&bytes)?;
        let mutation = as_mutation(schema, key.clone(), baseline.record);
        self.apply_replay_record(mutation, &self.writer_id).await?;
        if let Some(id) = self.pending_replay_record(schema, key).await? {
            self.clear_pending_replay_record(schema, key, &id).await?;
        }
        Ok(())
    }
}
