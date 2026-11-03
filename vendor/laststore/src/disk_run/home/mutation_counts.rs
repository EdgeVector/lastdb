//! Counts of the actual post-dedup mutations.
//!
//! Only fixed engine labels enter the report. Unknown collection names
//! collapse to `other`; record keys and values never enter diagnostics.

use crate::commit_timing as timing;

type Names = [&'static str; 4];

pub(super) fn record(records: &[(Vec<u8>, Option<Vec<u8>>)]) {
    if !timing::enabled() {
        return;
    }
    for (key, value) in records {
        // Every key was just built by run_key, with a checked u16 length.
        let length = u16::from_be_bytes([key[0], key[1]]) as usize;
        let at = 2 + length;
        let collection = std::str::from_utf8(&key[2..at]).unwrap_or_default();
        let names = names(collection);
        timing::count(names[usize::from(value.is_none())], 1);
        timing::count(names[2], key.len() as u64);
        timing::count(
            names[3],
            value.as_ref().map_or(0, |bytes| bytes.len()) as u64,
        );
    }
}

fn names(collection: &str) -> Names {
    match collection {
        "records" => [
            "v2_records_puts",
            "v2_records_deletes",
            "v2_records_key_bytes",
            "v2_records_value_bytes",
        ],
        "cas_blobs" => [
            "v2_cas_puts",
            "v2_cas_deletes",
            "v2_cas_key_bytes",
            "v2_cas_value_bytes",
        ],
        "field_keys" => [
            "v2_field_keys_puts",
            "v2_field_keys_deletes",
            "v2_field_keys_key_bytes",
            "v2_field_keys_value_bytes",
        ],
        "schemas" => [
            "v2_schemas_puts",
            "v2_schemas_deletes",
            "v2_schemas_key_bytes",
            "v2_schemas_value_bytes",
        ],
        "schema_states" => [
            "v2_sstates_puts",
            "v2_sstates_deletes",
            "v2_sstates_key_bytes",
            "v2_sstates_value_bytes",
        ],
        "db_catalog" => [
            "v2_catalog_puts",
            "v2_catalog_deletes",
            "v2_catalog_key_bytes",
            "v2_catalog_value_bytes",
        ],
        "metadata" => [
            "v2_metadata_puts",
            "v2_metadata_deletes",
            "v2_metadata_key_bytes",
            "v2_metadata_value_bytes",
        ],
        "__native_home" => [
            "v2_home_puts",
            "v2_home_deletes",
            "v2_home_key_bytes",
            "v2_home_value_bytes",
        ],
        _ => [
            "v2_other_puts",
            "v2_other_deletes",
            "v2_other_key_bytes",
            "v2_other_value_bytes",
        ],
    }
}
