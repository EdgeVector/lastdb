//! Apply `VerifiedHistory` LWW winners to real FoldDB records.
//!
//! Pin mapping (settle PR Q4): the history namespace is the FoldDB schema
//! name, the record key is the primary hash key, and the value is a UTF-8 JSON
//! object of the record's fields. An empty value is a tombstone that deletes
//! the record.

mod apply;
mod codec;
mod read;
mod schema;
mod store;

pub use apply::apply_history;
pub use codec::{decode_string_field, encode_string_field, STRING_FIELD};
pub use fold_db::fold_db_core::FoldDB;
pub use read::read_field_map;
pub use schema::ensure_hash_schema;
pub use store::{open_proof_store, open_store};
