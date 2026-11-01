//! Retire a bucket: remove its directory row when it is safe.
//!
//! A reader may retire a bucket after its walk has read every key of it. A
//! writer can add an entry to the bucket after the walk passed that place, so
//! the delete must be able to see the write. Every new entry rewrites the
//! directory row with a new token, in the batch that adds the entry. The reader
//! gives the row commit the token it read before the walk. The commit reads the
//! row in the write gate and deletes it only if it still holds that token. A
//! batch that added an entry after the walk changed the token, so the row stays,
//! and the reader starts the bucket again.
//!
//! The delete is a guarded delete of a row commit (`counter::GuardedDelete`). It
//! holds the write gate for one batch of one row, and it reads nothing but the
//! row.

use crate::db_operations::record_store::counter::{GuardedDelete, RowChanges};
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;

impl RecordStore {
    /// Delete the directory row `key` if it still holds `token`. Returns true
    /// when the row was deleted.
    pub(crate) async fn retire_bucket(
        &self,
        key: String,
        token: Vec<u8>,
    ) -> Result<bool, SchemaError> {
        self.in_background_scope(async {
            let commit = self
                .commit_rows(RowChanges {
                    guarded_deletes: vec![GuardedDelete {
                        key: key.into_bytes(),
                        expected: token,
                    }],
                    ..RowChanges::default()
                })
                .await?;
            Ok(commit.guarded_deleted > 0)
        })
        .await
    }
}
