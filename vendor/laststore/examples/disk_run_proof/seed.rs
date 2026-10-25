//! Synthetic brain-shaped seed for the disk run proof, from the disk size
//! spike (EdgeVector/lastdb#388): the measured schema shapes of the primary
//! on 2026-10-24, compact plain records, and today's OPE range encoding.
//! No primary or user data is read.
//!
//! lint:file-size-ok measured shape table and seed generator kept with the proof.
#![allow(dead_code)]

use sha2::{Digest, Sha256};

pub const MIB: f64 = 1024.0 * 1024.0;
const GROUPS: u64 = 1024;

// ---------------------------------------------------------------------------
// Measured shape of the primary on 2026-10-24 (`lastdb db schemas`, locator
// key census). Name, atom count, logical bytes, fields, range kind, derived.

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
    name: &'static str,
    atoms: usize,
    bytes: usize,
    fields: u32,
    range: Range,
    body: Body,
    /// A protein member of the content schemas replaces this schema.
    replaced_by_protein: bool,
}

const SHAPES: &[Shape] = &[
    shape(
        "BrainListPage",
        75_944,
        30_439_637,
        3,
        Range::Marker,
        Body::Json,
        true,
    ),
    shape(
        "Reference",
        20_111,
        17_285_478,
        10,
        Range::None,
        Body::Text,
        false,
    ),
    shape(
        "Papercut",
        18_556,
        15_071_079,
        10,
        Range::None,
        Body::Text,
        false,
    ),
    shape(
        "BrainListLocator",
        39_429,
        10_429_573,
        3,
        Range::Slug,
        Body::Json,
        true,
    ),
    shape(
        "BrainTokenSet",
        15_590,
        4_932_032,
        3,
        Range::Marker,
        Body::Json,
        false,
    ),
    shape(
        "BrainEdgeSet",
        9_457,
        3_080_328,
        3,
        Range::Marker,
        Body::Json,
        false,
    ),
    shape(
        "BrainTypeDirectory",
        8_172,
        2_411_000,
        3,
        Range::Marker,
        Body::Json,
        true,
    ),
    shape(
        "BrainGraphEdgeBySource",
        6_449,
        1_887_000,
        4,
        Range::Slug,
        Body::Json,
        false,
    ),
    shape("Project", 305, 687_800, 10, Range::None, Body::Text, false),
    shape("Design", 189, 639_300, 10, Range::None, Body::Text, false),
    shape("Decision", 751, 501_700, 12, Range::None, Body::Text, false),
    shape("Concept", 95, 231_400, 10, Range::None, Body::Text, false),
    shape(
        "LastSecret",
        589,
        156_000,
        6,
        Range::None,
        Body::Json,
        false,
    ),
    shape(
        "BrainBodyFile",
        403,
        135_700,
        4,
        Range::None,
        Body::Json,
        false,
    ),
    shape("Preference", 72, 64_900, 10, Range::None, Body::Text, false),
    shape(
        "LastSecretIndex",
        3,
        48_500,
        2,
        Range::None,
        Body::Json,
        false,
    ),
    shape("Spike", 7, 8_800, 10, Range::None, Body::Text, false),
    shape(
        "BrainListCount",
        20,
        4_900,
        2,
        Range::None,
        Body::Json,
        false,
    ),
    shape(
        "CompactionRecoveryReceipt",
        8,
        2_200,
        4,
        Range::None,
        Body::Json,
        false,
    ),
];

const fn shape(
    name: &'static str,
    atoms: usize,
    bytes: usize,
    fields: u32,
    range: Range,
    body: Body,
    replaced_by_protein: bool,
) -> Shape {
    Shape {
        name,
        atoms,
        bytes,
        fields,
        range,
        body,
        replaced_by_protein,
    }
}

/// Measured: 430,767 tips for 196,150 atoms.
const TIPS_PER_ATOM: f64 = 2.196;
/// Measured: 440,561 `atom:` keys = body + counter + this share of extra rows.
const EXTRA_ATOM_ROWS_PER_ATOM: f64 = 0.246;
/// Measured: 163,585 idempotency rows and 163,313 change feed rows.
const MUTATIONS_PER_ATOM: f64 = 0.834;
/// Record slugs (one BrainTypeDirectory atom per slug).
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

const WORDS: &[&str] = &[
    "the",
    "a",
    "to",
    "of",
    "and",
    "in",
    "is",
    "for",
    "on",
    "that",
    "with",
    "it",
    "this",
    "be",
    "not",
    "as",
    "by",
    "at",
    "or",
    "from",
    "when",
    "one",
    "read",
    "write",
    "key",
    "node",
    "schema",
    "field",
    "atom",
    "molecule",
    "hash",
    "range",
    "record",
    "brain",
    "card",
    "board",
    "LastDB",
    "disk",
    "memory",
    "cloud",
    "sync",
    "flush",
    "batch",
    "call",
    "socket",
    "error",
    "status",
    "routine",
    "agent",
    "PR",
    "merge",
    "main",
    "branch",
    "worktree",
    "CI",
    "check",
    "design",
    "decision",
    "papercut",
    "reference",
    "project",
    "concept",
    "spike",
    "Tom",
    "size",
    "bytes",
    "MiB",
    "file",
    "page",
    "pack",
    "locator",
    "tip",
    "counter",
    "seal",
    "encryption",
    "compression",
    "protein",
    "member",
    "index",
    "list",
    "slug",
    "type",
    "tag",
    "symptom",
    "fix",
    "fixed",
    "verified",
    "open",
    "closed",
    "repo",
    "commit",
    "test",
    "proof",
    "measured",
    "result",
    "current",
    "state",
    "change",
    "next",
    "action",
    "problem",
    "cause",
    "because",
    "after",
    "before",
    "then",
    "only",
    "every",
    "each",
    "all",
    "no",
    "new",
    "old",
    "time",
    "date",
    "2026-10-24",
    "seconds",
    "ms",
    "p95",
    "p50",
    "warm",
    "set",
    "purge",
    "fetch",
    "query",
    "mutation",
    "ack",
    "queue",
    "lane",
    "budget",
    "limit",
    "cap",
    "guard",
    "daemon",
    "Mini",
    "primary",
    "copy",
    "home",
    "upgrade",
    "release",
    "install",
    "binary",
    "kanban",
    "milestone",
    "North",
    "Star",
    "factory",
    "pickup",
    "review",
    "doing",
    "done",
    "`lastdb status`",
    "`brain get`",
    "`kanban show`",
    "src/lib.rs",
    "fold_db/crates/core",
    "returns",
    "writes",
    "reads",
    "stores",
    "keeps",
    "drops",
    "moves",
    "starts",
    "stops",
    "evidence",
    "symptom:",
    "repro:",
    "suggested",
    "owner",
    "severity",
    "p1",
    "p2",
    "p3",
];

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
    Tip {
        molecule: u32,
        hash: [u8; 16],
        range: Option<String>,
        atom: [u8; 32],
    },
    Idem {
        id: [u8; 32],
    },
    Event {
        seq: u64,
        atom: [u8; 32],
    },
}

pub struct Stream {
    pub items: Vec<Item>,
    pub logical_atom_bytes: u64,
    pub atoms: u64,
}

pub struct StreamOptions {
    pub proteins: bool,
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

pub fn build_stream(options: &StreamOptions) -> Stream {
    let mut rng = Rng(0x5eed_d15c_0001);
    let slugs = slug_pool(&mut rng);
    let mut items = Vec::new();
    let mut logical = 0u64;
    let mut atoms = 0u64;
    let mut molecule_base = 0u32;
    let mut content_atoms: Vec<[u8; 32]> = Vec::new();
    for (schema, shape) in SHAPES.iter().enumerate() {
        let base = molecule_base;
        // Up to three tips per atom, each in its own molecule, so no key repeats.
        molecule_base += shape.fields * 3;
        if options.proteins && shape.replaced_by_protein {
            continue;
        }
        let mean = shape.bytes as f64 / shape.atoms as f64;
        let sigma = if shape.body == Body::Text { 1.1 } else { 0.5 };
        for n in 0..shape.atoms {
            let size = rng.lognormal(mean, sigma).clamp(12.0, 48_000.0) as usize;
            let content = match shape.body {
                Body::Text => text(&mut rng, size),
                Body::Json => json_list(&mut rng, &slugs, size),
            };
            logical += content.len() as u64;
            atoms += 1;
            let id: [u8; 32] = rng.bytes();
            let field = n as u32 % shape.fields;
            let record = n / shape.fields as usize;
            let mut hash = [0u8; 16];
            hash[..8].copy_from_slice(&(schema as u64).to_le_bytes());
            hash[8..].copy_from_slice(&(record as u64).to_le_bytes());
            if shape.body == Body::Text {
                content_atoms.push(id);
            }
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
            if rng.unit() < EXTRA_ATOM_ROWS_PER_ATOM {
                items.push(Item::Extra { id });
            }
            let tips =
                TIPS_PER_ATOM.floor() as usize + usize::from(rng.unit() < TIPS_PER_ATOM.fract());
            for t in 0..tips {
                let range = match shape.range {
                    Range::None => None,
                    Range::Marker => Some(if n % 7 == 0 {
                        "__head__".to_string()
                    } else {
                        format!("p{:08}", n / 7)
                    }),
                    Range::Slug => Some(format!("reference#{}", slugs[record % slugs.len()])),
                };
                items.push(Item::Tip {
                    molecule: base + field * 3 + t as u32,
                    hash,
                    range,
                    atom: id,
                });
            }
        }
    }
    if options.proteins {
        // Members keyed by type and by type+status. Each member tip points at
        // the shared content atom: no atom copy, no page rewrite.
        for (n, slug) in slugs.iter().enumerate() {
            let atom = content_atoms[n % content_atoms.len()];
            for member in 0..2u32 {
                let mut hash = [0u8; 16];
                hash[..4].copy_from_slice(&member.to_le_bytes());
                hash[4] = (n % 9) as u8;
                items.push(Item::Tip {
                    molecule: molecule_base + member,
                    hash,
                    range: Some(slug.clone()),
                    atom,
                });
            }
        }
    }
    let mutations = (atoms as f64 * MUTATIONS_PER_ATOM) as u64;
    for seq in 0..mutations {
        items.push(Item::Idem { id: rng.bytes() });
        items.push(Item::Event {
            seq,
            atom: rng.bytes(),
        });
    }
    Stream {
        items,
        logical_atom_bytes: logical,
        atoms,
    }
}

// ---------------------------------------------------------------------------
// Shared encoders.

fn hex(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(H[(b >> 4) as usize] as char);
        out.push(H[(b & 15) as usize] as char);
    }
    out
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn molecule_id(molecule: u32) -> [u8; 32] {
    Sha256::digest(molecule.to_le_bytes()).into()
}

/// v1 OPE range: 8 hex characters per plaintext byte (`core/crypto/e2e.rs`).
fn ope_v1(molecule: u32, range: &str) -> String {
    let mut out = String::with_capacity(range.len() * 8);
    for (i, b) in range.bytes().enumerate() {
        let d = Sha256::digest([molecule.to_le_bytes(), (i as u32).to_le_bytes()].concat());
        let word = (u32::from_be_bytes([d[0], d[1], d[2], d[3]]) & 0xFFFF_FF00) | u32::from(b);
        out.push_str(&format!("{word:08x}"));
    }
    out
}

fn group_of(prefix: &[u8]) -> u16 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in prefix {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    (h % GROUPS) as u16
}

fn v2_key(item: &Item, ope_range: bool) -> Vec<u8> {
    let mut key = Vec::with_capacity(48);
    let (class, prefix_len) = match item {
        Item::Body { id, .. } => {
            key.extend_from_slice(id);
            (1u8, 32)
        }
        Item::Extra { id } => {
            key.extend_from_slice(id);
            key.push(b'h');
            (1, 32)
        }
        Item::Tip {
            molecule,
            hash,
            range,
            ..
        } => {
            key.extend_from_slice(&molecule.to_be_bytes());
            key.extend_from_slice(hash);
            let prefix = key.len();
            if let Some(range) = range {
                if ope_range {
                    key.extend_from_slice(ope_v1(*molecule, range).as_bytes());
                } else {
                    key.extend_from_slice(range.as_bytes());
                }
            }
            (2, prefix)
        }
        Item::Idem { id } => {
            key.extend_from_slice(id);
            (3, 32)
        }
        Item::Event { seq, .. } => {
            key.extend_from_slice(&seq.to_be_bytes());
            (4, 0)
        }
        Item::Counter { id, .. } => {
            key.extend_from_slice(id);
            key.push(b'c');
            (1, 32)
        }
    };
    let group = group_of(&key[..prefix_len]);
    let mut out = Vec::with_capacity(key.len() + 3);
    out.push(class);
    out.extend_from_slice(&group.to_be_bytes());
    out.extend_from_slice(&key);
    out
}

fn v2_value(item: &Item) -> Vec<u8> {
    let mut out = Vec::new();
    match item {
        Item::Body {
            schema,
            field,
            content,
            ..
        } => {
            varint(&mut out, u64::from(*schema));
            varint(&mut out, u64::from(*field));
            varint(&mut out, 1_792_802_400); // created_at, seconds
            varint(&mut out, 3); // writer number in the home's writer table
            out.extend_from_slice(content);
        }
        Item::Counter { bytes, .. } => varint(&mut out, u64::from(*bytes)),
        Item::Extra { id } => out.extend_from_slice(id),
        Item::Tip { atom, .. } => {
            out.extend_from_slice(atom);
            varint(&mut out, 1);
        }
        Item::Idem { id } => {
            out.extend_from_slice(&id[..16]);
            varint(&mut out, 200);
        }
        Item::Event { atom, .. } => {
            out.extend_from_slice(atom);
            varint(&mut out, 1_792_802_400);
        }
    }
    out
}

pub fn v2_records_in_write_order(stream: &Stream, ope_range: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let events = stream
        .items
        .iter()
        .filter(|i| matches!(i, Item::Event { .. }))
        .count() as u64;
    let mut idem = 0u64;
    stream
        .items
        .iter()
        .filter(|item| match item {
            Item::Counter { .. } => false,
            Item::Idem { .. } => {
                idem += 1;
                idem % 500 == 0
            }
            Item::Event { seq, .. } => *seq + 10_000 >= events,
            _ => true,
        })
        .map(|item| (v2_key(item, ope_range), v2_value(item)))
        .collect()
}
