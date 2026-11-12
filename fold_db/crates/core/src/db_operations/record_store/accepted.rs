//! Stage one accepted note for each user mutation before the native commit.

use super::RecordStore;
use crate::schema::types::{Mutation, SchemaError};

impl RecordStore {
    pub(crate) async fn stage_accepted_mutations(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let format = &self.format;
        crate::storage::disk_backup::accepted::stage_records(
            &self.runtime,
            format.content_key(),
            |reference| format.admit_local(reference),
            mutations,
        )
        .await?;
        Ok(())
    }
}
