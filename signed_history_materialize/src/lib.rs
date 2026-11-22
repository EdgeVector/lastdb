//! Apply `VerifiedHistory` LWW winners to real FoldDB records.
//!
//! Pin mapping (settle PR Q4): the history namespace is the FoldDB schema
//! name, the record key is the primary hash key, and the value is a UTF-8 JSON
//! object of the record's fields. An empty value is a tombstone that deletes
//! the record. Schema definitions come from signed `schema` entries.

mod apply;
mod codec;
mod declare;
mod read;
mod schema;
mod store;

pub use apply::apply_history;
pub use codec::{decode_string_field, encode_string_field, STRING_FIELD};
pub use declare::declare_history_schemas;
pub use fold_db::fold_db_core::FoldDB;
pub use read::read_field_map;
pub use schema::{ensure_hash_schema, hash_string_schema_bytes, hash_string_schema_json};
pub use store::{open_proof_store, open_store};
