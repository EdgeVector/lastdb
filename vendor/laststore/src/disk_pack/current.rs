use super::bytes::*;
use super::{corrupt, TreeKind, TreeRoot};
use crate::Result;

/// One immutable committed directory root. No mutable object is captured here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Current {
    /// Monotonically increasing publication generation.
    pub generation: u64,
    /// Prior generation; zero for the first publication.
    pub previous_generation: u64,
    /// Last complete readable mutation batch.
    pub frontier: u64,
    /// Exact directory root.
    pub directory: TreeRoot,
    /// Placement codec identity from the home descriptor.
    pub placement_codec: u16,
    /// Storage-key codec identity from the home descriptor.
    pub key_codec: u16,
    /// Publication time in Unix milliseconds.
    pub published_ms: u64,
    /// Mandatory format features. Unknown bits cause an early refusal.
    pub required_features: u64,
    /// SHA-256 of the complete native home descriptor; zero for separate artifacts.
    pub home_digest: [u8; 32],
}

impl Current {
    /// Complete ordered-operation ceiling selected by this held durable root.
    /// Native homes select the extended ceiling; standalone artifacts keep the original.
    pub fn batch_operation_limit(&self) -> usize {
        batch_operation_limit(self.required_features)
    }

    /// Decode a complete fixed-width CURRENT; refuse unsupported features.
    pub fn decode(bytes: &[u8; 4096]) -> Result<Self> {
        if &bytes[..8] != b"LDCUR1\0\0"
            || u16_at(bytes, 8) != 1
            || u16_at(bytes, 10) != 4096
            || u16_at(bytes, 74) != 1024
            || u64_at(bytes, 104) != 0
        {
            return Err(corrupt("unsupported CURRENT format or feature"));
        }
        zero(&bytes[12..16])?;
        verify_checksum(bytes, 128)?;
        require_snapshot_features(u64_at(bytes, 112))?;
        zero(&bytes[40..48])?;
        zero(&bytes[80..96])?;
        zero(&bytes[192..])?;
        let current = Self {
            generation: u64_at(bytes, 16),
            previous_generation: u64_at(bytes, 24),
            frontier: u64_at(bytes, 32),
            directory: TreeRoot {
                object: nonnil(uuid_at(bytes, 48))?,
                page: u64_at(bytes, 64),
                height: u16_at(bytes, 72)
                    .try_into()
                    .map_err(|_| corrupt("tree height"))?,
                kind: TreeKind::Directory,
            },
            placement_codec: u16_at(bytes, 76),
            key_codec: u16_at(bytes, 78),
            published_ms: u64_at(bytes, 120),
            required_features: u64_at(bytes, 112),
            home_digest: bytes[160..192].try_into().expect("fixed home digest"),
        };
        current.validate()?;
        if u64_at(bytes, 96) != current.frontier {
            return Err(corrupt("checkpoint differs from readable frontier"));
        }
        Ok(current)
    }

    /// Encode the complete object, including its checksum and zero slack.
    pub fn encode(&self) -> Result<[u8; 4096]> {
        self.validate()?;
        let mut bytes = [0; 4096];
        bytes[..8].copy_from_slice(b"LDCUR1\0\0");
        put16(&mut bytes, 8, 1);
        put16(&mut bytes, 10, 4096);
        put64(&mut bytes, 16, self.generation);
        put64(&mut bytes, 24, self.previous_generation);
        put64(&mut bytes, 32, self.frontier);
        bytes[48..64].copy_from_slice(self.directory.object.as_bytes());
        put64(&mut bytes, 64, self.directory.page);
        put16(&mut bytes, 72, self.directory.height.into());
        put16(&mut bytes, 74, 1024);
        put16(&mut bytes, 76, self.placement_codec);
        put16(&mut bytes, 78, self.key_codec);
        put64(&mut bytes, 96, self.frontier);
        put64(&mut bytes, 120, self.published_ms);
        put64(&mut bytes, 112, self.required_features);
        bytes[160..192].copy_from_slice(&self.home_digest);
        let sum = checksum(&bytes, 128);
        bytes[128..160].copy_from_slice(&sum);
        Ok(bytes)
    }

    fn validate(&self) -> Result<()> {
        require_snapshot_features(self.required_features)?;
        if self.required_features & super::FEATURE_PACKED_KEYS == 0 {
            return Err(crate::Error::Config(
                "unsupported locator format: packed-key feature required".into(),
            ));
        }
        if self.required_features & super::FEATURE_NATIVE_HOME != 0
            && self.required_features & super::FEATURE_ATOM_RECORDS == 0
        {
            return Err(crate::Error::Config(
                "unsupported atom record layout: this home has no atom-record feature \
                 and was made by another build"
                    .into(),
            ));
        }
        self.directory.validate()?;
        if self.generation == 0
            || self.previous_generation >= self.generation
            || self.directory.kind != TreeKind::Directory
            || self.required_features & !super::SUPPORTED_FEATURES != 0
            || (self.required_features
                & (super::FEATURE_SCHEMA_RECORD_REFS
                    | super::FEATURE_LARGE_BATCHES
                    | super::FEATURE_ATOM_RECORDS)
                != 0
                && self.required_features & super::FEATURE_NATIVE_HOME == 0)
            || ((self.required_features & super::FEATURE_NATIVE_HOME != 0)
                != (self.home_digest != [0; 32]))
            || self.required_features & super::FEATURE_OBJECT_METADATA == 0
        {
            return Err(corrupt("inconsistent CURRENT frontier or generation"));
        }
        Ok(())
    }
}

fn require_snapshot_features(features: u64) -> Result<()> {
    // Check the retired journal format before the unknown-feature fallback.
    // Old roots must receive an explicit refusal, never an automatic conversion.
    if features & super::FEATURE_SNAPSHOT_COMMIT == 0 || features & 2 != 0 {
        return Err(crate::Error::Config(
            "unsupported internal-journal disk format: an explicit snapshot conversion is required; \
             startup does not migrate existing data"
                .into(),
        ));
    }
    if features & !super::SUPPORTED_FEATURES != 0 {
        return Err(corrupt("unsupported CURRENT feature"));
    }
    Ok(())
}

pub(super) fn batch_operation_limit(required_features: u64) -> usize {
    if required_features & super::FEATURE_LARGE_BATCHES != 0 {
        super::LARGE_BATCH_OPERATIONS
    } else {
        super::LEGACY_BATCH_OPERATIONS
    }
}
