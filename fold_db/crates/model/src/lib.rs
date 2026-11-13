//! Pure LastDB data model: records, schemas, keys, mutations, and canonical framing.

pub mod canonical;
pub mod field_key_codec;
pub mod portable_mutation;
pub mod record_value;
pub mod schema;

pub use crate::canonical::{content_limit, hex, kind_partition, storage_policy};
pub use crate::field_key_codec::{field_key_id, record_replacement, record_space};
