//! Deterministic synthetic commits and the model of what they leave.
//!
//! Batch `n` always holds the same operations, so a parent process can
//! rebuild the state a killed child left after any number of commits.

use laststore::disk_run::home::HomeMutation;
use std::collections::BTreeMap;

/// Expected visible state per (collection, key); `None` is a deleted key.
pub type Model = BTreeMap<(String, String), Option<Vec<u8>>>;

pub const RECORD_SCHEMAS: u64 = 40;
pub const RECORD_RANGES: u64 = 4000;
pub const FIELD_KEYS: u64 = 200;

const WORDS: [&str; 16] = [
    "entry", "schema", "field", "stamp", "range", "hash", "blob", "record", "brain", "kanban",
    "decision", "design", "note", "update", "value", "key",
];

pub struct Rng(u64);

impl Rng {
    /// Scramble the seed first: SplitMix64 states from nearby seeds are
    /// shifted copies of one sequence, so batch n and n + 1 would repeat
    /// each other's keys.
    pub fn new(seed: u64) -> Self {
        let mut rng = Self(seed);
        let start = rng.next_u64();
        Self(start)
    }

    pub fn next_u64(&mut self) -> u64 {
        // SplitMix64.
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

pub fn record_key(schema: u64, range: u64) -> String {
    format!("schema{schema:03}\0{range:08}")
}

fn field_key_entry(field_key: u64, range: u64) -> String {
    format!("fk{field_key:04}\0{range:010}")
}

/// A brain-like JSON text value; it compresses about as real records do.
fn value(rng: &mut Rng, batch: u64) -> Vec<u8> {
    let words = 12 + rng.below(60);
    let mut text = String::new();
    for _ in 0..words {
        text.push_str(WORDS[rng.below(WORDS.len() as u64) as usize]);
        text.push(' ');
    }
    format!(
        "{{\"batch\":{batch},\"id\":\"{:016x}\",\"text\":\"{}\"}}",
        rng.next_u64(),
        text.trim_end()
    )
    .into_bytes()
}

/// The operations of batch `n` (1-based). About 60% new blob counts, 25%
/// record writes (many overwrite), 10% field key entries, 5% record deletes.
pub fn batch(n: u64, ops: usize) -> Vec<HomeMutation> {
    let mut rng = Rng::new(n);
    let mut out = Vec::with_capacity(ops);
    for op in 0..ops {
        let pick = rng.below(100);
        let (collection, key, body) = if pick < 60 {
            let key = format!("{n:08x}{op:06x}{:08x}", rng.next_u64() as u32);
            ("blob_counts", key, Some(value(&mut rng, n)))
        } else if pick < 85 {
            let key = record_key(rng.below(RECORD_SCHEMAS), rng.below(RECORD_RANGES));
            ("records", key, Some(value(&mut rng, n)))
        } else if pick < 95 {
            let key = field_key_entry(rng.below(FIELD_KEYS), rng.next_u64() % 10_000_000_000);
            ("field_keys", key, Some(format!("{n}:{op}").into_bytes()))
        } else {
            let key = record_key(rng.below(RECORD_SCHEMAS), rng.below(RECORD_RANGES));
            ("records", key, None)
        };
        out.push(HomeMutation {
            collection: collection.to_string(),
            key,
            body,
        });
    }
    out
}

/// Batch `n` of the overwrite-heavy phase: every operation hits one of
/// 20,000 record keys, so most writes replace a value or delete it (10%).
pub fn overwrite_batch(n: u64, ops: usize) -> Vec<HomeMutation> {
    let mut rng = Rng::new(n ^ 0x5eed_0f0f);
    (0..ops)
        .map(|_| {
            let key = record_key(rng.below(5), rng.below(RECORD_RANGES));
            let body = match rng.below(10) {
                0 => None,
                _ => Some(value(&mut rng, n)),
            };
            HomeMutation {
                collection: "records".to_string(),
                key,
                body,
            }
        })
        .collect()
}

/// Apply a batch to the model in order; a later operation wins.
pub fn apply(model: &mut Model, operations: &[HomeMutation]) {
    for op in operations {
        model.insert((op.collection.clone(), op.key.clone()), op.body.clone());
    }
}

/// Key and value bytes of the live records: the logical size.
pub fn logical_bytes(model: &Model) -> u64 {
    model
        .iter()
        .filter_map(|((c, k), v)| v.as_ref().map(|v| (c.len() + k.len() + v.len()) as u64))
        .sum()
}
