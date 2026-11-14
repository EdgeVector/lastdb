//! Pure LastDB data model: records, schemas, keys, mutations, and canonical framing.

pub mod canonical;
pub mod field_key_codec;
pub mod keys;
pub mod limits;
pub mod portable_mutation;
pub mod record_value;
pub mod schema;

pub use crate::canonical::hex;
pub use crate::field_key_codec::{field_key_id, record_replacement, record_space};
pub use crate::keys::kind_partition;
pub use crate::limits::{content_limit, storage_policy};
