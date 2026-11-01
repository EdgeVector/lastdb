//! The key codecs, value limits and value rules of the data model.
//!
//! A format 7 home stores records (`rk:`), not atoms. The molecule key codec
//! still names each alternate key and record range: a molecule identity, a
//! blinded hash and a typed range. `atom_key_codec` recognizes a format 6
//! atom row so that the restore proof can name it when it refuses it.

pub mod atom_key_codec;
pub mod content_limit;
pub mod file_pointer;
pub mod molecule_key_codec;
pub mod molecule_uuid;
pub mod provenance;
pub mod tombstone;

pub use content_limit::{
    atom_content_byte_len, ABSOLUTE_MAX_ATOM_CONTENT_BYTES, ATOM_CONTENT_TOO_LARGE_HINT,
    DEFAULT_MAX_ATOM_CONTENT_BYTES, HEADROOM_ALARM_FRACTION, MAX_ATOM_CONTENT_BYTES_ENV,
    MIN_MAX_ATOM_CONTENT_BYTES, RECORD_TOO_LARGE_HINT,
};
pub use molecule_key_codec::{MoleculeKeyCodec, MoleculeKeyCodecError};
pub use molecule_uuid::encode_molecule_uuid_bytes;
pub use provenance::{MoleculeRef, Provenance};
pub use tombstone::{is_tombstone_value, TOMBSTONE_KEY};

/// Generates a deterministic molecule UUID from schema name and field name.
///
/// Identity is SHA-256 of the framed domain, schema and field tuple from
/// [`molecule_uuid::molecule_uuid_digest`], encoded as unpadded base64url.
/// Logical reads and writes use that exact string, without alternate spellings.
pub fn deterministic_molecule_uuid(schema_name: &str, field_name: &str) -> String {
    molecule_uuid::encode_molecule_uuid_bytes(&molecule_uuid::molecule_uuid_digest(
        schema_name,
        field_name,
    ))
}
