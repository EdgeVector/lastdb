//! The record provider delegates file encoding/reading to its format capability.

use super::RecordStore;
use crate::error::FoldDbError;
use crate::schema::types::FileWrite;
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_value::FileMarker;

impl RecordStore {
    pub(crate) fn seal_record_file(
        &self,
        plaintext: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        self.format.seal_record_file(plaintext, name, media_type)
    }

    pub(crate) async fn open_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        self.format
            .open_record_file(record_key, version, marker)
            .await
    }
}
