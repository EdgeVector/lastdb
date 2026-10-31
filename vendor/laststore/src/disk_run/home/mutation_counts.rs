//! Counts of the actual post-dedup mutations, before value encoding.
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
        if collection == "atoms" {
            let tag = match key[at..].rsplit(|byte| *byte == 0).next() {
                Some(b"b") => ["v2_atom_body_puts", "v2_atom_body_deletes"],
                Some(b"c") => ["v2_atom_counter_puts", "v2_atom_counter_deletes"],
                _ => ["v2_atom_other_puts", "v2_atom_other_deletes"],
            };
            timing::count(tag[usize::from(value.is_none())], 1);
        }
    }
}

fn names(collection: &str) -> Names {
    match collection {
        "atoms" => [
            "v2_atoms_puts",
            "v2_atoms_deletes",
            "v2_atoms_key_bytes",
            "v2_atoms_value_bytes",
        ],
        "tips" => [
            "v2_tips_puts",
            "v2_tips_deletes",
            "v2_tips_key_bytes",
            "v2_tips_value_bytes",
        ],
        "molecule_ref_edges" => [
            "v2_mref_puts",
            "v2_mref_deletes",
            "v2_mref_key_bytes",
            "v2_mref_value_bytes",
        ],
        "indexes" => [
            "v2_indexes_puts",
            "v2_indexes_deletes",
            "v2_indexes_key_bytes",
            "v2_indexes_value_bytes",
        ],
        "cas_blobs" => [
            "v2_cas_puts",
            "v2_cas_deletes",
            "v2_cas_key_bytes",
            "v2_cas_value_bytes",
        ],
        "molecule_keys" => [
            "v2_mkeys_puts",
            "v2_mkeys_deletes",
            "v2_mkeys_key_bytes",
            "v2_mkeys_value_bytes",
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
