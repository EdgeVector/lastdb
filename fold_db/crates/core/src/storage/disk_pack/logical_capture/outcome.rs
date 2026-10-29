//! One certified final logical field effect, independent of native run layout.

use crate::atom::{Atom, AtomEntry, KeyMetadata};
use crate::schema::types::KeyValue;
use serde::{Deserialize, Serialize};

/// A local accepted Put. The outer device event signs this complete outcome.
/// It carries no caller predicate, native storage key, or reference counter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedFieldOutcome {
    pub schema_name: String,
    pub schema_identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternate_key_revision: Option<String>,
    pub field_name: String,
    pub field_identity: String,
    pub molecule_uuid: String,
    pub key: KeyValue,
    /// The exact stored body, including metadata retained by atom deduplication.
    pub atom: Atom,
    /// The exact staged tip, preserving its original authored order.
    pub tip: AtomEntry,
    pub metadata: Option<KeyMetadata>,
}

/// Identical wire fields borrowed for the pre-clone memory admission check.
#[derive(Serialize)]
pub(crate) struct AcceptedFieldOutcomeRef<'a> {
    pub schema_name: &'a str,
    pub schema_identity: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alternate_key_revision: Option<&'a str>,
    pub field_name: &'a str,
    pub field_identity: &'a str,
    pub molecule_uuid: &'a str,
    pub key: &'a KeyValue,
    pub atom: &'a Atom,
    pub tip: &'a AtomEntry,
    pub metadata: Option<&'a KeyMetadata>,
}

impl AcceptedFieldOutcomeRef<'_> {
    pub(crate) fn into_owned(self) -> AcceptedFieldOutcome {
        AcceptedFieldOutcome {
            schema_name: self.schema_name.to_owned(),
            schema_identity: self.schema_identity.to_owned(),
            alternate_key_revision: self.alternate_key_revision.map(str::to_owned),
            field_name: self.field_name.to_owned(),
            field_identity: self.field_identity.to_owned(),
            molecule_uuid: self.molecule_uuid.to_owned(),
            key: self.key.clone(),
            atom: self.atom.clone(),
            tip: self.tip.clone(),
            metadata: self.metadata.cloned(),
        }
    }
}
