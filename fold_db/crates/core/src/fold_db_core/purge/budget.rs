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
//! Each tip that an erase removes costs these operations at most:
//!
//! - the tip delete,
//! - the `mgd` marker that records the removed key,
//! - the body delete and the counter delete, when this tip holds the last
//!   reference to its atom (a shared atom costs a counter write instead),
//! - for a delete, one barrier row.
//!
//! Measured on a fresh home with a unique value in every field: a purge of
//! 16 keys costs 3.8 operations per tip at 6 fields and 4.0 at 24 fields. A
//! delete costs 4.8 and 5.0. Each chunk of keys adds one header write for each
//! field and a few rows (the ledger row and the order log).
//!
//! The reads cost about 10 to 12 examined keys for each tip of a purge and one
//! more for each tip of a delete (the target tips, the peer expansion, the counters and the
//! body records). The read budget, not the operation limit, is the smaller
//! bound for a purge of more than 6 fields on a fresh home.
//!
//! A schema with named alternate keys costs more for each key. The erase
//! deletes at most one entry for each alternate key. It reads the fields that
//! derive those keys twice, before and after the tip removal, and each read is
//! a tip and its atom. One request also holds at most
//! [`MAX_ALTERNATE_ENTRIES`] of those field reads and of those entry changes,
//! across all its chunks.

use crate::alternate_keys::MAX_ALTERNATE_ENTRIES;
use crate::schema::types::Schema;

/// Keys in one chunk of an erase. A chunk reads its tips, plans, and stages.
pub const ERASE_CHUNK_KEYS: usize = 64;
/// Operations of a purge for each tip, at most.
pub const PURGE_OPERATIONS_PER_TIP: usize = 4;
/// Operations of a delete for each tip, at most.
pub const DELETE_OPERATIONS_PER_TIP: usize = 5;
/// Rows of a chunk that do not depend on the tips: the ledger row and others.
const CHUNK_FIXED_OPERATIONS: usize = 8;
/// Examined keys of all reads in one write request (the write-preparation
/// budget of a native read call, `PackRuntime::physical_read` in
/// `storage/disk_pack/physical.rs`).
pub const WRITE_REQUEST_READ_KEYS: usize = 40_000;
/// Examined keys for each tip of a purge, at most (measured 9.1 to 10.1 at 6 to
/// 24 fields and 11.8 at 300 fields).
pub const PURGE_READS_PER_TIP: usize = 12;
/// Examined keys for each tip of a delete, at most (one more than a purge).
pub const DELETE_READS_PER_TIP: usize = 13;
/// Examined keys for each alternate source field of each key, at most: the tip
/// and its atom, read before and after the erase.
pub const ALTERNATE_READS_PER_FIELD: usize = 4;

/// The parts of a schema that set the cost of erasing one key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EraseShape {
    /// Fields of the schema. Each key costs one tip for each field.
    pub fields: usize,
    /// Named alternate keys. Each key costs one entry delete for each.
    pub alternates: usize,
    /// Fields that derive the alternate keys, with the primary hash field.
    /// Zero when the schema has no alternate key.
    pub alternate_fields: usize,
}

impl EraseShape {
    /// The shape of `schema` as an erase reads and writes it.
    #[must_use]
    pub fn of(schema: &Schema) -> Self {
        let alternates = schema.alternate_keys.len();
        let alternate_fields = if alternates == 0 {
            0
        } else {
            schema.alternate_dependency_fields().len()
        };
        Self {
            fields: schema.runtime_fields.len(),
            alternates,
            alternate_fields,
        }
    }
}

fn per_tip(delete: bool) -> usize {
    if delete {
        DELETE_OPERATIONS_PER_TIP
    } else {
        PURGE_OPERATIONS_PER_TIP
    }
}

fn reads_per_tip(delete: bool) -> usize {
    if delete {
        DELETE_READS_PER_TIP
    } else {
        PURGE_READS_PER_TIP
    }
}

/// The most operations that an erase of `keys` keys of a schema with `shape`
/// stages in one request.
#[must_use]
pub fn erase_operations_upper_bound(delete: bool, keys: usize, shape: EraseShape) -> usize {
    let chunks = keys.div_ceil(ERASE_CHUNK_KEYS);
    keys.saturating_mul(shape.fields)
        .saturating_mul(per_tip(delete))
        .saturating_add(chunks.saturating_mul(shape.fields.saturating_add(CHUNK_FIXED_OPERATIONS)))
        .saturating_add(keys.saturating_mul(shape.alternates))
}

/// The most examined keys that an erase of `keys` keys of a schema with `shape`
/// reads in one request.
#[must_use]
pub fn erase_reads_upper_bound(delete: bool, keys: usize, shape: EraseShape) -> usize {
    let chunks = keys.div_ceil(ERASE_CHUNK_KEYS);
    keys.saturating_mul(shape.fields)
        .saturating_mul(reads_per_tip(delete))
        .saturating_add(chunks.saturating_mul(shape.fields.saturating_add(CHUNK_FIXED_OPERATIONS)))
        .saturating_add(
            keys.saturating_mul(shape.alternate_fields)
                .saturating_mul(ALTERNATE_READS_PER_FIELD),
        )
}

fn fits_alternate_caps(keys: usize, shape: EraseShape) -> bool {
    keys.saturating_mul(shape.alternate_fields) <= MAX_ALTERNATE_ENTRIES
        && keys.saturating_mul(shape.alternates) <= MAX_ALTERNATE_ENTRIES
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
    let mut keys = (limit / width.saturating_mul(per_tip(delete)))
        .min(WRITE_REQUEST_READ_KEYS / width.saturating_mul(reads_per_tip(delete)))
        .min(MAX_ALTERNATE_ENTRIES / shape.alternate_fields.max(1))
        .min(MAX_ALTERNATE_ENTRIES / shape.alternates.max(1));
    while keys > 0 && !fits(delete, keys, shape, limit) {
        keys -= 1;
    }
    keys
}
