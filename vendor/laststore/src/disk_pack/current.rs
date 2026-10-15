use super::bytes::*;
use super::{corrupt, TreeKind, TreeRoot};
use crate::Result;
use uuid::Uuid;

/// One immutable committed directory root. No mutable object is captured here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Current {
    /// Monotonically increasing publication generation.
    pub generation: u64,
    /// Prior generation; zero for the first publication.
    pub previous_generation: u64,
    /// Last complete readable mutation batch.
    pub frontier: u64,
    /// Durable journal boundary known at publication.
    pub journal_frontier: u64,
    /// Exact directory root.
    pub directory: TreeRoot,
    /// Placement codec identity from the home descriptor.
    pub placement_codec: u16,
    /// Storage-key codec identity from the home descriptor.
    pub key_codec: u16,
    /// Recovery journal identity at this boundary.
    pub journal_stream: Uuid,
    /// Publication time in Unix milliseconds.
    pub published_ms: u64,
}

impl Current {
    /// Decode a complete fixed-width CURRENT; refuse unsupported features.
    pub fn decode(bytes: &[u8; 4096]) -> Result<Self> {
        if &bytes[..8] != b"LDCUR1\0\0"
            || u16_at(bytes, 8) != 1
            || u16_at(bytes, 10) != 4096
            || u16_at(bytes, 74) != 1024
            || u64_at(bytes, 104) != 0
            || u64_at(bytes, 112) != 0
        {
            return Err(corrupt("unsupported CURRENT format or feature"));
        }
        zero(&bytes[12..16])?;
        zero(&bytes[160..])?;
        verify_checksum(bytes, 128)?;
        let current = Self {
            generation: u64_at(bytes, 16),
            previous_generation: u64_at(bytes, 24),
            frontier: u64_at(bytes, 32),
            journal_frontier: u64_at(bytes, 40),
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
            journal_stream: nonnil(uuid_at(bytes, 80))?,
            published_ms: u64_at(bytes, 120),
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
        put64(&mut bytes, 40, self.journal_frontier);
        bytes[48..64].copy_from_slice(self.directory.object.as_bytes());
        put64(&mut bytes, 64, self.directory.page);
        put16(&mut bytes, 72, self.directory.height.into());
        put16(&mut bytes, 74, 1024);
        put16(&mut bytes, 76, self.placement_codec);
        put16(&mut bytes, 78, self.key_codec);
        bytes[80..96].copy_from_slice(self.journal_stream.as_bytes());
        put64(&mut bytes, 96, self.frontier);
        put64(&mut bytes, 120, self.published_ms);
        let sum = checksum(&bytes, 128);
        bytes[128..160].copy_from_slice(&sum);
        Ok(bytes)
    }

    fn validate(&self) -> Result<()> {
        self.directory.validate()?;
        nonnil(self.journal_stream)?;
        if self.generation == 0
            || self.previous_generation >= self.generation
            || self.frontier > self.journal_frontier
            || self.directory.kind != TreeKind::Directory
        {
            return Err(corrupt("inconsistent CURRENT frontier or generation"));
        }
        Ok(())
    }
}
