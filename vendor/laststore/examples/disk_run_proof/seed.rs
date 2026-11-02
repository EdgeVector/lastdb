//! Synthetic brain-shaped seed for the disk run proof, from the disk size
//! spike (EdgeVector/lastdb#388): the measured schema shapes of the primary
//! on 2026-10-24, compact plain records, and today's OPE range encoding.
//! No primary or user data is read.
//!

use sha2::{Digest, Sha256};

pub const MIB: f64 = 1024.0 * 1024.0;
const GROUPS: u64 = 1024;

// ---------------------------------------------------------------------------
// Measured shape of the primary on 2026-10-24 (`lastdb db schemas`, locator
// key census). Name, value count, logical bytes, fields, range kind, derived.

#[derive(Clone, Copy, PartialEq, Eq)]
enum Range {
    /// Hash schema: no range.
    None,
    /// A short fixed marker such as `__head__` or `p00000001`.
    Marker,
    /// `type#slug`, about 40 bytes.
    Slug,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Body {
    Text,
    Json,
}

struct Shape {
    values: usize,
    bytes: usize,
    fields: u32,
    range: Range,
    body: Body,
}

const SHAPES: &[Shape] = &include!("seed/shapes.rs");

const fn shape(
    _name: &'static str,
    values: usize,
    bytes: usize,
    fields: u32,
    range: Range,
    body: Body,
) -> Shape {
    Shape {
        values,
        bytes,
        fields,
        range,
        body,
    }
}

/// Measured: 430,767 key entries for 196,150 values.
const ENTRIES_PER_VALUE: f64 = 2.196;
/// Measured: 440,561 value keys = body + counter + this share of extra rows.
const EXTRA_ROWS_PER_VALUE: f64 = 0.246;
/// Measured: 163,585 idempotency rows and 163,313 change feed rows.
const MUTATIONS_PER_VALUE: f64 = 0.834;
/// Record slugs (one BrainTypeDirectory value per slug).
const RECORDS: usize = 8_172;

// ---------------------------------------------------------------------------
// Deterministic generator.

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn normal(&mut self) -> f64 {
        let a = self.unit().max(1e-12);
        let b = self.unit();
        (-2.0 * a.ln()).sqrt() * (std::f64::consts::TAU * b).cos()
    }
    fn lognormal(&mut self, mean: f64, sigma: f64) -> f64 {
        (mean.ln() - sigma * sigma / 2.0 + sigma * self.normal()).exp()
    }
    fn bytes<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        for chunk in out.chunks_mut(8) {
            let word = self.next().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
        out
    }
    /// Zipf-like index: small indexes are much more common.
    fn zipf(&mut self, n: usize) -> usize {
        let u = self.unit();
        ((n as f64).powf(u) - 1.0) as usize % n
    }
}

const WORDS: &[&str] = &include!("seed/words.rs");

fn text(rng: &mut Rng, len: usize) -> Vec<u8> {
    let mut out = String::with_capacity(len + 32);
    let mut sentence = 0;
    while out.len() < len {
        out.push_str(WORDS[rng.zipf(WORDS.len())]);
        sentence += 1;
        if sentence > 6 + rng.below(12) {
            sentence = 0;
            out.push_str(if rng.below(8) == 0 { ".\n\n## " } else { ". " });
        } else {
            out.push(' ');
        }
    }
    out.truncate(len);
    out.into_bytes()
}

fn json_list(rng: &mut Rng, slugs: &[String], len: usize) -> Vec<u8> {
    let mut out = String::with_capacity(len + 96);
    out.push_str("{\"items\":[");
    while out.len() < len {
        let slug = &slugs[rng.zipf(slugs.len())];
        out.push_str("{\"t\":\"");
        out.push_str(["reference", "papercut", "decision", "design"][rng.below(4)]);
        out.push_str("\",\"s\":\"");
        out.push_str(slug);
        out.push_str("\",\"n\":\"");
        for _ in 0..3 + rng.below(5) {
            out.push_str(WORDS[rng.zipf(WORDS.len())]);
            out.push(' ');
        }
        out.push_str("\"},");
    }
    out.truncate(len);
    out.into_bytes()
}

// ---------------------------------------------------------------------------
// Logical stream. Keys and values hold logical parts; each layout encodes them.

#[derive(Clone)]
pub enum Item {
    Body {
        id: [u8; 32],
        schema: u32,
        field: u32,
        content: Vec<u8>,
    },
    Counter {
        id: [u8; 32],
        bytes: u32,
    },
    Extra {
        id: [u8; 32],
    },
    Entry {
        field_key: u32,
        hash: [u8; 16],
        range: Option<String>,
        value_id: [u8; 32],
    },
    Idem {
        id: [u8; 32],
    },
    Event {
        seq: u64,
        value_id: [u8; 32],
    },
}

pub struct Stream {
    pub items: Vec<Item>,
    pub logical_value_bytes: u64,
    pub values: u64,
}

fn slug_pool(rng: &mut Rng) -> Vec<String> {
    (0..RECORDS)
        .map(|n| {
            format!(
                "{}-{}-{}-{n:04}",
                WORDS[rng.below(WORDS.len())]
                    .to_lowercase()
                    .replace(['`', ' ', '/', ':'], ""),
                WORDS[rng.below(WORDS.len())]
                    .to_lowercase()
                    .replace(['`', ' ', '/', ':'], ""),
                WORDS[rng.below(WORDS.len())]
                    .to_lowercase()
                    .replace(['`', ' ', '/', ':'], ""),
            )
        })
        .collect()
}

pub fn build_stream() -> Stream {
    let mut rng = Rng(0x5eed_d15c_0001);
    let slugs = slug_pool(&mut rng);
    let mut items = Vec::new();
    let mut logical = 0u64;
    let mut values = 0u64;
    let mut field_key_base = 0u32;
    for (schema, shape) in SHAPES.iter().enumerate() {
        let base = field_key_base;
        // Up to three entries per value, each under its own field key, so no key repeats.
        field_key_base += shape.fields * 3;
        let mean = shape.bytes as f64 / shape.values as f64;
        let sigma = if shape.body == Body::Text { 1.1 } else { 0.5 };
        for n in 0..shape.values {
            let size = rng.lognormal(mean, sigma).clamp(12.0, 48_000.0) as usize;
            let content = match shape.body {
                Body::Text => text(&mut rng, size),
                Body::Json => json_list(&mut rng, &slugs, size),
            };
            logical += content.len() as u64;
            values += 1;
            let id: [u8; 32] = rng.bytes();
            let field = n as u32 % shape.fields;
            let record = n / shape.fields as usize;
            let mut hash = [0u8; 16];
            hash[..8].copy_from_slice(&(schema as u64).to_le_bytes());
            hash[8..].copy_from_slice(&(record as u64).to_le_bytes());
            items.push(Item::Body {
                id,
                schema: schema as u32,
                field,
                content,
            });
            items.push(Item::Counter {
                id,
                bytes: size as u32,
            });
            if rng.unit() < EXTRA_ROWS_PER_VALUE {
                items.push(Item::Extra { id });
            }
            let entries = ENTRIES_PER_VALUE.floor() as usize
                + usize::from(rng.unit() < ENTRIES_PER_VALUE.fract());
            for t in 0..entries {
                let range = match shape.range {
                    Range::None => None,
                    Range::Marker => Some(if n % 7 == 0 {
                        "__head__".to_string()
                    } else {
                        format!("p{:08}", n / 7)
                    }),
                    Range::Slug => Some(format!("reference#{}", slugs[record % slugs.len()])),
                };
                items.push(Item::Entry {
                    field_key: base + field * 3 + t as u32,
                    hash,
                    range,
                    value_id: id,
                });
            }
        }
    }
    let mutations = (values as f64 * MUTATIONS_PER_VALUE) as u64;
    for seq in 0..mutations {
        items.push(Item::Idem { id: rng.bytes() });
        items.push(Item::Event {
            seq,
            value_id: rng.bytes(),
        });
    }
    Stream {
        items,
        logical_value_bytes: logical,
        values,
    }
}

// ---------------------------------------------------------------------------
// Shared encoders.

mod encode;
pub use encode::v2_records_in_write_order;
