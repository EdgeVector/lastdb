//! Historical conflict identifiers keep their source key domain after conversion.
//!
//! This representation is used only to reproduce a retained history identifier.
//! Typed lookup keys always use their ordered payload inside encrypted blocks.

use super::{HashKeyEncoding, MoleculeKeyCodec, MoleculeKeyCodecError, RangeKeyEncoding};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConflictKeyIdentity {
    #[default]
    StoredV1,
    LegacyV1 {
        hash: HashKeyEncoding,
        range: RangeKeyEncoding,
    },
}

impl MoleculeKeyCodec {
    pub(crate) fn with_conflict_identity(&self, identity: ConflictKeyIdentity) -> Self {
        Self {
            conflict_identity: identity,
            ..self.clone()
        }
    }

    pub(crate) fn conflict_identity(&self) -> ConflictKeyIdentity {
        self.conflict_identity
    }

    pub(crate) fn historical_conflict_coordinate(
        &self,
        molecule: &str,
        hash: &str,
        range: &str,
    ) -> Result<Option<(String, String)>, MoleculeKeyCodecError> {
        let ConflictKeyIdentity::LegacyV1 {
            hash: hash_encoding,
            range: range_encoding,
        } = self.conflict_identity
        else {
            return Ok(None);
        };
        let legacy = Self {
            hash_encoding,
            range_encoding,
            range_profile: None,
            ..self.clone()
        };
        Ok(Some((
            legacy.storage_hash(molecule, hash)?,
            legacy.storage_range(molecule, range)?,
        )))
    }
}
