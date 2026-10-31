//! Compact complete-atom certificates; the caller owns the decoded body.

use super::*;
use lastdb_model::canonical::{push_field, CanonicalValue};
use sha2::{Digest, Sha256};

/// Bind every Atom field, including optional metadata and its physical time.
/// This certificate authorizes reuse only within the call's recorded source.
/// It is separate from the content UUID, which excludes these metadata fields.
pub(super) fn fingerprint(atom: &Atom) -> StorageResult<[u8; 32]> {
    let metadata = atom
        .metadata()
        .map(|metadata| metadata.iter().collect::<BTreeMap<_, _>>());
    let mut sink = DigestSink(Sha256::new());
    push_field(&mut sink.0, b"lastdb-native-full-atom-v1");
    // The fixed JSON tuple preserves field boundaries, None versus empty,
    // sorted metadata/object maps, and the declared order of array members.
    serde_json::to_writer(
        &mut sink,
        &(
            atom.uuid(),
            atom.source_schema_name(),
            atom.source_file_name(),
            metadata,
            atom.created_at(),
            CanonicalValue(atom.content()),
        ),
    )
    .map_err(|error| serialization(&error))?;
    Ok(sink.0.finalize().into())
}

struct DigestSink(Sha256);

impl std::io::Write for DigestSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
