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

use crate::storage::StorageError;

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

fn per_tip(delete: bool) -> usize {
    if delete {
        DELETE_OPERATIONS_PER_TIP
    } else {
        PURGE_OPERATIONS_PER_TIP
    }
}

/// The most operations that an erase of `keys` keys of a schema with `fields`
/// fields stages in one request.
#[must_use]
pub fn erase_operations_upper_bound(delete: bool, keys: usize, fields: usize) -> usize {
    let chunks = keys.div_ceil(ERASE_CHUNK_KEYS);
    keys.saturating_mul(fields)
        .saturating_mul(per_tip(delete))
        .saturating_add(chunks.saturating_mul(fields.saturating_add(CHUNK_FIXED_OPERATIONS)))
}

/// The most examined keys that an erase of `keys` keys of a schema with
/// `fields` fields reads in one request.
#[must_use]
pub fn erase_reads_upper_bound(delete: bool, keys: usize, fields: usize) -> usize {
    let per_tip = if delete {
        DELETE_READS_PER_TIP
    } else {
        PURGE_READS_PER_TIP
    };
    let chunks = keys.div_ceil(ERASE_CHUNK_KEYS);
    keys.saturating_mul(fields)
        .saturating_mul(per_tip)
        .saturating_add(chunks.saturating_mul(fields.saturating_add(CHUNK_FIXED_OPERATIONS)))
}

fn fits(delete: bool, keys: usize, fields: usize, limit: usize) -> bool {
    erase_operations_upper_bound(delete, keys, fields) <= limit
        && erase_reads_upper_bound(delete, keys, fields) <= WRITE_REQUEST_READ_KEYS
}

/// The most keys of a schema with `fields` fields that one request can erase
/// within `limit` operations and the read budget. Zero means that not even one
/// key fits.
#[must_use]
pub fn max_erase_keys(delete: bool, fields: usize, limit: usize) -> usize {
    let reads_per_tip = if delete {
        DELETE_READS_PER_TIP
    } else {
        PURGE_READS_PER_TIP
    };
    let width = fields.max(1);
    let mut keys = (limit / width.saturating_mul(per_tip(delete)))
        .min(WRITE_REQUEST_READ_KEYS / width.saturating_mul(reads_per_tip));
    while keys > 0 && !fits(delete, keys, fields, limit) {
        keys -= 1;
    }
    keys
}

/// Refuse an erase request that cannot fit in one batch. The caller runs this
/// before it takes the write gate or stages any row.
pub(crate) fn check_erase_size(
    delete: bool,
    keys: usize,
    fields: usize,
    limit: usize,
) -> Result<(), StorageError> {
    if fits(delete, keys, fields, limit) {
        return Ok(());
    }
    let needed = erase_operations_upper_bound(delete, keys, fields);
    let reads = erase_reads_upper_bound(delete, keys, fields);
    let verb = if delete { "delete" } else { "purge" };
    let max = max_erase_keys(delete, fields, limit);
    let advice = if max == 0 {
        format!("a schema of {fields} fields is too wide for one {verb} request")
    } else {
        format!("send at most {max} keys of {fields} fields in one request")
    };
    Err(StorageError::MutationLimit {
        detail: format!(
            "this {verb} of {keys} keys of {fields} fields needs up to {needed} storage \
             operations and {reads} reads; one request holds at most {limit} operations \
             and {WRITE_REQUEST_READ_KEYS} reads; {advice}"
        ),
    })
}
