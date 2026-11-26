//! Open sealed data and schema bodies held in [`VerifiedHistory`].

use std::sync::Arc;

use crate::entry::{DataBody, EntryHeader, SchemaBody, SignedEntry};
use crate::payload::{decode_record_payload, RecordPayload};
use crate::sealed::{open_data_body, open_schema_body, SealOpener};

use super::VerifiedHistory;

impl VerifiedHistory {
    /// Install the opener that unlocks sealed data and schema payloads.
    ///
    /// Accepted entries stay sealed on disk and in [`AcceptedMeta`]; this opener
    /// is used only when applying effects, recomputing, or reading content.
    pub fn set_opener(&mut self, opener: Arc<dyn SealOpener>) {
        self.opener = Some(opener);
    }

    /// Open a data body to `(namespace, payload)`. Sealed bodies need the
    /// installed opener; clear bodies pass through for in-process proofs.
    pub fn open_data(&self, header: &EntryHeader, body: &DataBody) -> Option<(String, Vec<u8>)> {
        open_data_body(self.opener.as_deref(), header, body)
    }

    /// Open a schema body to `(namespace, definition)`. Sealed bodies need the
    /// installed opener; clear bodies pass through for in-process proofs.
    pub fn open_schema(
        &self,
        header: &EntryHeader,
        body: &SchemaBody,
    ) -> Option<(String, Vec<u8>)> {
        open_schema_body(self.opener.as_deref(), header, body)
    }

    /// Decode the record payload of a sealed data entry.
    pub fn decode_data_entry(&self, entry: &SignedEntry) -> Option<(String, RecordPayload)> {
        let SignedEntry::Data { header, body } = entry else {
            return None;
        };
        let (namespace, payload) = self.open_data(header, body)?;
        let decoded = decode_record_payload(&payload).ok()?;
        Some((namespace, decoded))
    }
}
