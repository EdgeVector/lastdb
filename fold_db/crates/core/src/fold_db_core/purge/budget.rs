//! The size of one erase request, counted before the write gate.
//!
//! One erase request stages all its chunks of keys in one native batch. A
//! batch holds at most 16,384 operations.
//! The reads of the request, inside the write gate, share one budget of 40,000
//! examined keys. A request that is too big used to fail after it staged two
//! chunks, with a message that named no remedy. This module counts the
//! operations and the reads first, so the request is refused early and the
//! message says how many keys to send.
//!
//! The bound charges each erased key a fixed number of operations and
//! examined keys for each field of its schema, and each chunk of keys a few
//! more rows (the ledger row and others). These are upper bounds: an erase of
//! one record stages its record delete and its alternate entry deletes.
//!
//! A schema with named alternate keys stages at most one entry delete for
//! each explicit address. No payload fields are read to derive these keys.

use crate::alternate_keys::MAX_ALTERNATE_ENTRIES;
use crate::schema::types::Schema;

/// Keys in one chunk of an erase. A chunk reads its records, plans, and stages.
pub const ERASE_CHUNK_KEYS: usize = 64;
/// Operations of a purge for each field of each key, at most.
pub const PURGE_OPERATIONS_PER_FIELD: usize = 4;
/// Operations of a delete for each field of each key, at most.
pub const DELETE_OPERATIONS_PER_FIELD: usize = 5;
/// Rows of a chunk that do not depend on the keys: the ledger row and others.
const CHUNK_FIXED_OPERATIONS: usize = 8;
/// Examined keys of all reads in one write request (the write-preparation
/// budget of a native read call, `PackRuntime::physical_read` in
/// `storage/disk_pack/physical.rs`).
pub const WRITE_REQUEST_READ_KEYS: usize = 40_000;
/// Examined keys for each field of each key of a purge, at most.
pub const PURGE_READS_PER_FIELD: usize = 12;
/// Examined keys for each field of each key of a delete, at most.
pub const DELETE_READS_PER_FIELD: usize = 13;
/// The parts of a schema that set the cost of erasing one key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EraseShape {
    /// Fields of the schema. Each key costs a bound for each field.
    pub fields: usize,
    /// Named alternate keys. Each key costs one entry delete for each.
    pub alternates: usize,
}

impl EraseShape {
    /// The shape of `schema` as an erase reads and writes it.
    #[must_use]
    pub fn of(schema: &Schema) -> Self {
        let alternates = schema
            .record_keys
            .as_ref()
            .map_or(0, |keys| keys.alternates.len());
        Self {
            fields: schema.runtime_fields.len().max(1),
            alternates,
        }
    }
}

fn operations_per_field(delete: bool) -> usize {
    if delete {
        DELETE_OPERATIONS_PER_FIELD
    } else {
        PURGE_OPERATIONS_PER_FIELD
    }
}

fn reads_per_field(delete: bool) -> usize {
    if delete {
        DELETE_READS_PER_FIELD
    } else {
        PURGE_READS_PER_FIELD
    }
}

/// The most operations that an erase of `keys` keys of a schema with `shape`
/// stages in one request.
#[must_use]
pub fn erase_operations_upper_bound(delete: bool, keys: usize, shape: EraseShape) -> usize {
    let chunks = keys.div_ceil(ERASE_CHUNK_KEYS);
    keys.saturating_mul(shape.fields)
        .saturating_mul(operations_per_field(delete))
        .saturating_add(chunks.saturating_mul(shape.fields.saturating_add(CHUNK_FIXED_OPERATIONS)))
        .saturating_add(keys.saturating_mul(shape.alternates))
}

/// The most examined keys that an erase of `keys` keys of a schema with `shape`
/// reads in one request.
#[must_use]
pub fn erase_reads_upper_bound(delete: bool, keys: usize, shape: EraseShape) -> usize {
    let chunks = keys.div_ceil(ERASE_CHUNK_KEYS);
    keys.saturating_mul(shape.fields)
        .saturating_mul(reads_per_field(delete))
        .saturating_add(chunks.saturating_mul(shape.fields.saturating_add(CHUNK_FIXED_OPERATIONS)))
}

fn fits_alternate_caps(keys: usize, shape: EraseShape) -> bool {
    keys.saturating_mul(shape.alternates) <= MAX_ALTERNATE_ENTRIES
}

fn fits(delete: bool, keys: usize, shape: EraseShape, limit: usize) -> bool {
    erase_operations_upper_bound(delete, keys, shape) <= limit
        && erase_reads_upper_bound(delete, keys, shape) <= WRITE_REQUEST_READ_KEYS
        && fits_alternate_caps(keys, shape)
}

/// The most keys of a schema with `shape` that one request can erase within
/// `limit` operations, the read budget, and the alternate entry caps. Zero
/// means that not even one key fits.
#[must_use]
pub fn max_erase_keys(delete: bool, shape: EraseShape, limit: usize) -> usize {
    let width = shape.fields.max(1);
    let mut keys = (limit / width.saturating_mul(operations_per_field(delete)))
        .min(WRITE_REQUEST_READ_KEYS / width.saturating_mul(reads_per_field(delete)))
        .min(MAX_ALTERNATE_ENTRIES / shape.alternates.max(1));
    while keys > 0 && !fits(delete, keys, shape, limit) {
        keys -= 1;
    }
    keys
}
