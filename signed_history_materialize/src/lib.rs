//! Apply `VerifiedHistory` LWW winners to real FoldDB records.
//!
//! Pin mapping (settle PR Q4): the history namespace is the FoldDB schema
//! name, the record key is the primary key (a hash key, or the hash and range joined
//! by `mutation_log::RANGE_SEPARATOR` for a hash-range schema), and the value is a UTF-8 JSON
//! object of the record's fields. An empty value is a tombstone that deletes
//! the record. Schema definitions come from signed `schema` entries.

mod apply;
mod attachments;
mod codec;
mod declare;
mod key;
mod read;
mod schema;
mod store;
mod validate;
mod value;

pub use apply::{
    apply_history, apply_record_winner, apply_winner_with_file, prepare_external_file,
    reconcile_keys,
};
pub use attachments::FileSection;
pub use codec::{decode_string_field, encode_string_field, STRING_FIELD};
pub use declare::declare_history_schemas;
pub use fold_db::fold_db_core::FoldDB;
pub use key::{canonical_record_key, canonical_record_key_from_joined, key_value};
pub use read::{read_alternates, read_field_map, read_revision};
pub use schema::{ensure_hash_schema, hash_string_schema_bytes, hash_string_schema_json};
pub use store::{open_proof_store, open_store};
pub use validate::{check_shared_record_size, check_shared_record_size_with_attachments};
pub use value::{
    decode_record_value, encode_record_value, resolve_alternates, Alternates, DecodedRecord,
};
