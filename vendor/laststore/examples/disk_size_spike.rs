//! Disk size design spike (design-lastdb-disk-size-20261024). CI only.
//!
//! The primary held 84 MiB of logical atoms in 865 MiB of `data/` on
//! 2026-10-24. This spike builds a synthetic brain-shaped stream with the
//! measured counts and sizes, writes it in the v1 native format (the real
//! `DiskHome` writer) and in candidate v2 layouts, reads every candidate back,
//! and prints bytes on disk divided by logical atom bytes. No primary or user
//! data is opened. The goal is a ratio of at most 2.0.
//!
//! lint:file-size-ok one self-contained CI design spike, removed when Tom decides the v2 format.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use laststore::disk_pack::home::{DiskHome, HomeBuilder, HomeDescriptor, HomeMutation};
use laststore::disk_pack::ReadBudget;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const MIB: f64 = 1024.0 * 1024.0;
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
enum Item {
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

struct Stream {
    items: Vec<Item>,
    logical_atom_bytes: u64,
    atoms: u64,
}

struct StreamOptions {
    proteins: bool,
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

fn build_stream(options: &StreamOptions) -> Stream {
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

fn b64(bytes: &[u8], pad: bool) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = match chunk.len() {
            3 => (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]),
            2 => (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8),
            _ => u32::from(chunk[0]) << 16,
        };
        let emit = chunk.len() + 1;
        for i in 0..4 {
            if i < emit {
                out.push(A[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else if pad {
                out.push('=');
            }
        }
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

fn read_varint(buf: &[u8], at: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = buf[*at];
        *at += 1;
        v |= u64::from(b & 0x7f) << shift;
        if b < 0x80 {
            return v;
        }
        shift += 7;
    }
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

struct Sealer {
    cipher: Aes256Gcm,
    counter: u64,
}

impl Sealer {
    fn new() -> Self {
        Self {
            cipher: Aes256Gcm::new_from_slice(&[7u8; 32]).expect("key"),
            counter: 0,
        }
    }
    /// Returns nonce (12) + ciphertext + tag (16).
    fn seal(&mut self, plaintext: &[u8]) -> Vec<u8> {
        self.counter += 1;
        let mut nonce = [0u8; 12];
        nonce[4..].copy_from_slice(&self.counter.to_be_bytes());
        let ct = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce), plaintext)
            .expect("seal");
        let mut out = Vec::with_capacity(12 + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        out
    }
    fn open(&self, sealed: &[u8]) -> Vec<u8> {
        self.cipher
            .decrypt(Nonce::from_slice(&sealed[..12]), &sealed[12..])
            .expect("open")
    }
}

fn group_of(prefix: &[u8]) -> u16 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in prefix {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    (h % GROUPS) as u16
}

// ---------------------------------------------------------------------------
// v1: the real native writer, with the node's key and value encodings.

struct V1Options {
    /// `LASTDB_KV_AT_REST_RAW`: `ENB:` binary in place of `ENC:` base64.
    raw_seal: bool,
    /// Raw range bytes in place of the 8x OPE hex.
    raw_range: bool,
    /// No counter row while the count is one.
    implied_counter: bool,
    /// One idempotency row per 500 mutations; keep the last 10,000 events.
    bounded_bookkeeping: bool,
}

fn v1_collection(item: &Item) -> &'static str {
    match item {
        Item::Body { .. } | Item::Counter { .. } | Item::Extra { .. } => "atoms",
        Item::Tip { .. } => "tips",
        Item::Idem { .. } => "idempotency",
        Item::Event { .. } => "change_feed",
    }
}

fn v1_key(item: &Item, options: &V1Options) -> String {
    match item {
        Item::Body { id, .. } => format!("atom:{}\0b", hex(id)),
        Item::Counter { id, .. } => format!("atom:{}\0c", hex(id)),
        Item::Extra { id } => format!("atom:{}\0h", hex(id)),
        Item::Tip {
            molecule,
            hash,
            range,
            ..
        } => {
            let mut key = format!(
                "mk:{}:{}",
                b64(&molecule_id(*molecule), false),
                b64(hash, false)
            );
            if let Some(range) = range {
                key.push('\0');
                if options.raw_range {
                    key.push_str(range);
                } else {
                    key.push_str(&ope_v1(*molecule, range));
                }
            }
            key
        }
        Item::Idem { id } => format!("idem:{}", hex(id)),
        Item::Event { seq, .. } => format!("event:{seq:020}"),
    }
}

/// Plaintext the node seals, shaped like the JSON rows it writes.
fn v1_plaintext(item: &Item, sealer: &mut Sealer) -> Vec<u8> {
    let writer = hex(&[3u8; 32]);
    match item {
        Item::Body { schema, field, content, .. } => {
            // JSON header + binary `ENB:` inner content (`atom/content_at_rest.rs`).
            let inner = sealer.seal(content);
            let mut out = format!(
                "{{\"schema\":\"{}\",\"field\":\"f{field}\",\"writer\":\"{writer}\",\"created_at\":\"2026-10-24T14:20:00.000000Z\",\"content_len\":{},\"content\":\"",
                hex(&molecule_id(*schema)),
                content.len()
            )
            .into_bytes();
            out.extend_from_slice(b"ENB:\x00");
            out.extend_from_slice(&[1u8]);
            out.extend_from_slice(&inner);
            out.extend_from_slice(b"\"}");
            out
        }
        Item::Counter { bytes, .. } => {
            format!("{{\"refs\":1,\"bytes\":{bytes},\"suspect\":false,\"v\":1}}").into_bytes()
        }
        Item::Extra { id } => format!("{{\"successor\":\"{}\"}}", hex(id)).into_bytes(),
        Item::Tip { atom, .. } => format!(
            "{{\"atom\":\"{}\",\"version\":1,\"writer\":\"{writer}\",\"ts\":1792802400000}}",
            hex(atom)
        )
        .into_bytes(),
        Item::Idem { id } => {
            format!("{{\"digest\":\"{}\",\"status\":200,\"ts\":1792802400000}}", hex(id))
                .into_bytes()
        }
        Item::Event { seq, atom } => format!(
            "{{\"seq\":{seq},\"kind\":\"mutation\",\"schema\":\"{}\",\"atom\":\"{}\",\"ts\":1792802400000}}",
            hex(&molecule_id(*seq as u32)),
            hex(atom)
        )
        .into_bytes(),
    }
}

/// `encrypting_store::seal_value`: compress at 256 B or more when that gets
/// to 75% or less (deflate in the node; zstd level 1 stands in), then seal.
fn v1_seal(plaintext: &[u8], raw: bool, sealer: &mut Sealer) -> Vec<u8> {
    let compressed = if plaintext.len() >= 256 {
        zstd::bulk::compress(plaintext, 1)
            .ok()
            .filter(|c| c.len() * 4 <= plaintext.len() * 3)
    } else {
        None
    };
    let deflated = compressed.is_some();
    let body = compressed.unwrap_or_else(|| plaintext.to_vec());
    let mut envelope = vec![1u8];
    envelope.extend_from_slice(&sealer.seal(&body));
    if raw {
        let mut out = b"ENB:".to_vec();
        out.push(u8::from(deflated));
        out.extend_from_slice(&envelope);
        out
    } else {
        let prefix = if deflated { "ENZ:" } else { "ENC:" };
        format!("{prefix}{}", b64(&envelope, true)).into_bytes()
    }
}

fn v1_items<'a>(stream: &'a Stream, options: &V1Options) -> Vec<&'a Item> {
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
            Item::Counter { .. } => !options.implied_counter,
            Item::Idem { .. } if options.bounded_bookkeeping => {
                idem += 1;
                idem % 500 == 0
            }
            Item::Event { seq, .. } if options.bounded_bookkeeping => *seq + 10_000 >= events,
            _ => true,
        })
        .collect()
}

fn write_v1(root: &Path, stream: &Stream, options: &V1Options) -> Result<Measure> {
    let start = Instant::now();
    HomeBuilder::create_empty_with_schema_record_refs(root, HomeDescriptor::new(1, 0)?)?;
    let home = DiskHome::open(root)?;
    let mut sealer = Sealer::new();
    let items = v1_items(stream, options);
    let mut sample = Vec::new();
    let mut batch = Vec::with_capacity(4096);
    for (n, item) in items.iter().enumerate() {
        let collection = v1_collection(item).to_string();
        let key = v1_key(item, options);
        let body = v1_seal(
            &v1_plaintext(item, &mut sealer),
            options.raw_seal,
            &mut sealer,
        );
        if n % 509 == 0 {
            sample.push((collection.clone(), key.clone(), body.clone()));
        }
        batch.push(HomeMutation {
            collection,
            key,
            body: Some(body),
        });
        if batch.len() == 4096 {
            commit(&home, std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        commit(&home, batch)?;
    }
    let write_secs = start.elapsed().as_secs_f64();
    // Read back a sample through the real reader.
    let selected = home.read(ReadBudget {
        pages: 4096,
        blocks: 16384,
        bytes: 256 * 1024 * 1024,
        keys: 10_000,
    })?;
    let limits = ReadBudget::for_current(selected.current());
    drop(selected);
    let mut verified = 0;
    for chunk in sample.chunks(64) {
        let coordinates: Vec<_> = chunk
            .iter()
            .map(|(c, k, _)| (c.clone(), k.clone()))
            .collect();
        let mut read = home.read(limits)?;
        for (found, (_, key, body)) in read.get_many(&coordinates)?.into_iter().zip(chunk) {
            if found?.as_deref() != Some(body.as_slice()) {
                return Err(format!("v1 read-back differs for {}", key.escape_debug()).into());
            }
            verified += 1;
        }
    }
    drop(home);
    let mut measure = measure_dir(root)?;
    measure.write_secs = write_secs;
    measure.keys = items.len() as u64;
    measure.verified = format!("{verified} sampled keys equal");
    Ok(measure)
}

fn commit(home: &DiskHome, operations: Vec<HomeMutation>) -> Result<()> {
    let mut snapshot = home.snapshot()?;
    let reservation = snapshot.reserve_mutation(&operations)?;
    snapshot.commit_mutation(operations, reservation)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// v2 candidate: one sorted run per home, front-coded keys, binary values,
// compress-then-seal per block. Two locator shapes: dense (one small entry
// per key) and sparse (first key per block).

#[derive(Clone, Copy)]
struct V2Options {
    block_bytes: usize,
    zstd_level: i32,
    dense_locator: bool,
}

/// Sort key = collection byte, group (2 B), stored key. 1,024 groups (C11).
fn v2_key(item: &Item) -> Vec<u8> {
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
                key.extend_from_slice(range.as_bytes());
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

fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

struct BlockRef {
    first_key: Vec<u8>,
    offset: u64,
    len: u32,
}

fn write_sealed(file: &mut fs::File, sealer: &mut Sealer, raw: &[u8], level: i32) -> Result<u32> {
    let compressed = zstd::bulk::compress(raw, level)?;
    let sealed = sealer.seal(&compressed);
    file.write_all(&(sealed.len() as u32).to_le_bytes())?;
    file.write_all(&sealed)?;
    Ok(sealed.len() as u32 + 4)
}

fn read_sealed(bytes: &[u8], offset: u64, sealer: &Sealer, cap: usize) -> Result<Vec<u8>> {
    let at = offset as usize;
    let len = u32::from_le_bytes(bytes[at..at + 4].try_into()?) as usize;
    let compressed = sealer.open(&bytes[at + 4..at + 4 + len]);
    Ok(zstd::bulk::decompress(&compressed, cap)?)
}

/// Decode one block: (key, value) pairs.
fn decode_block(raw: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut at = 0;
    let mut prev: Vec<u8> = Vec::new();
    while at < raw.len() {
        let shared = read_varint(raw, &mut at) as usize;
        let suffix = read_varint(raw, &mut at) as usize;
        let mut key = prev[..shared].to_vec();
        key.extend_from_slice(&raw[at..at + suffix]);
        at += suffix;
        let vlen = read_varint(raw, &mut at) as usize;
        let value = raw[at..at + vlen].to_vec();
        at += vlen;
        prev.clone_from(&key);
        out.push((key, value));
    }
    out
}

fn v2_items(stream: &Stream) -> Vec<(Vec<u8>, Vec<u8>)> {
    let events = stream
        .items
        .iter()
        .filter(|i| matches!(i, Item::Event { .. }))
        .count() as u64;
    let mut idem = 0u64;
    let mut records: Vec<_> = stream
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
        .map(|item| (v2_key(item), v2_value(item)))
        .collect();
    records.sort_by(|a, b| a.0.cmp(&b.0));
    records
}

fn write_v2(root: &Path, stream: &Stream, options: V2Options) -> Result<Measure> {
    // lint:fn-size-ok spike driver kept linear so the measured steps read in order.
    let start = Instant::now();
    fs::DirBuilder::new().mode(0o700).create(root)?;
    let records = v2_items(stream);
    let mut sealer = Sealer::new();
    let mut pack = fs::File::create(root.join("pack"))?;
    let mut blocks: Vec<BlockRef> = Vec::new();
    // (key, block number, index in block) for the dense locator.
    let mut entries: Vec<(usize, u32)> = Vec::with_capacity(records.len());
    let mut raw = Vec::with_capacity(options.block_bytes + 4096);
    let mut prev: &[u8] = &[];
    let mut first = 0usize;
    let mut in_block = 0u32;
    let mut offset = 0u64;
    for (n, (key, value)) in records.iter().enumerate() {
        if raw.is_empty() {
            first = n;
            prev = &[];
            in_block = 0;
        }
        let shared = shared_prefix(prev, key);
        varint(&mut raw, shared as u64);
        varint(&mut raw, (key.len() - shared) as u64);
        raw.extend_from_slice(&key[shared..]);
        varint(&mut raw, value.len() as u64);
        raw.extend_from_slice(value);
        entries.push((blocks.len(), in_block));
        in_block += 1;
        prev = key.as_slice();
        if raw.len() >= options.block_bytes || n + 1 == records.len() {
            let len = write_sealed(&mut pack, &mut sealer, &raw, options.zstd_level)?;
            blocks.push(BlockRef {
                first_key: records[first].0.clone(),
                offset,
                len,
            });
            offset += u64::from(len);
            raw.clear();
        }
    }
    pack.sync_all()?;
    drop(pack);
    // Locator.
    let mut locator = fs::File::create(root.join("locator"))?;
    let mut page_index: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut loc_offset = 0u64;
    if options.dense_locator {
        let mut page = Vec::with_capacity(16 * 1024 + 256);
        let mut prev: &[u8] = &[];
        let mut first = 0usize;
        for (n, (key, _)) in records.iter().enumerate() {
            if page.is_empty() {
                first = n;
                prev = &[];
            }
            let shared = shared_prefix(prev, key);
            varint(&mut page, shared as u64);
            varint(&mut page, (key.len() - shared) as u64);
            page.extend_from_slice(&key[shared..]);
            varint(&mut page, entries[n].0 as u64);
            varint(&mut page, u64::from(entries[n].1));
            prev = key.as_slice();
            if page.len() >= 16 * 1024 - 64 || n + 1 == records.len() {
                // Pages are sealed but not compressed: a page read must stay cheap.
                let sealed = sealer.seal(&page);
                locator.write_all(&(sealed.len() as u32).to_le_bytes())?;
                locator.write_all(&sealed)?;
                page_index.push((records[first].0.clone(), loc_offset));
                loc_offset += sealed.len() as u64 + 4;
                page.clear();
            }
        }
    }
    // Root index: first key of each locator page (dense) or each block (sparse).
    let mut index = Vec::new();
    let roots: Vec<(&[u8], u64, u32)> = if options.dense_locator {
        page_index
            .iter()
            .map(|(k, o)| (k.as_slice(), *o, 0))
            .collect()
    } else {
        blocks
            .iter()
            .map(|b| (b.first_key.as_slice(), b.offset, b.len))
            .collect()
    };
    let mut prev: &[u8] = &[];
    for (key, offset, len) in &roots {
        let shared = shared_prefix(prev, key);
        varint(&mut index, shared as u64);
        varint(&mut index, (key.len() - shared) as u64);
        index.extend_from_slice(&key[shared..]);
        varint(&mut index, *offset);
        varint(&mut index, u64::from(*len));
        prev = *key;
    }
    fs::write(
        root.join("root"),
        sealer.seal(&zstd::bulk::compress(&index, 3)?),
    )?;
    if options.dense_locator {
        let mut block_table = Vec::new();
        for block in &blocks {
            varint(&mut block_table, block.offset);
            varint(&mut block_table, u64::from(block.len));
        }
        fs::write(
            root.join("blocks"),
            sealer.seal(&zstd::bulk::compress(&block_table, 3)?),
        )?;
    }
    locator.sync_all()?;
    drop(locator);
    let write_secs = start.elapsed().as_secs_f64();
    let verified = verify_v2(root, &records, &blocks, &page_index, options, &sealer)?;
    let mut measure = measure_dir(root)?;
    measure.write_secs = write_secs;
    measure.keys = records.len() as u64;
    measure.verified = verified;
    Ok(measure)
}

/// Read every block back and compare all records; then point gets by key.
fn verify_v2(
    root: &Path,
    records: &[(Vec<u8>, Vec<u8>)],
    blocks: &[BlockRef],
    pages: &[(Vec<u8>, u64)],
    options: V2Options,
    sealer: &Sealer,
) -> Result<String> {
    let pack = fs::read(root.join("pack"))?;
    let cap = options.block_bytes * 2 + 64 * 1024;
    let mut n = 0;
    for block in blocks {
        for (key, value) in decode_block(&read_sealed(&pack, block.offset, sealer, cap)?) {
            if records[n].0 != key || records[n].1 != value {
                return Err(format!("v2 full read differs at record {n}").into());
            }
            n += 1;
        }
    }
    if n != records.len() {
        return Err(format!("v2 full read found {n} of {} records", records.len()).into());
    }
    let locator = if options.dense_locator {
        fs::read(root.join("locator"))?
    } else {
        Vec::new()
    };
    let mut rng = Rng(0xfeed);
    let gets = 20_000.min(records.len());
    let mut block_cache: HashMap<usize, Vec<(Vec<u8>, Vec<u8>)>> = HashMap::new();
    for _ in 0..gets {
        let (key, value) = &records[rng.below(records.len())];
        let found = if options.dense_locator {
            let p = pages.partition_point(|(first, _)| first.as_slice() <= key.as_slice()) - 1;
            let at = pages[p].1 as usize;
            let len = u32::from_le_bytes(locator[at..at + 4].try_into()?) as usize;
            let page = sealer.open(&locator[at + 4..at + 4 + len]);
            let mut i = 0;
            let mut prev: Vec<u8> = Vec::new();
            let mut location = None;
            while i < page.len() {
                let shared = read_varint(&page, &mut i) as usize;
                let suffix = read_varint(&page, &mut i) as usize;
                let mut k = prev[..shared].to_vec();
                k.extend_from_slice(&page[i..i + suffix]);
                i += suffix;
                let block = read_varint(&page, &mut i) as usize;
                let slot = read_varint(&page, &mut i) as usize;
                if &k == key {
                    location = Some((block, slot));
                    break;
                }
                prev = k;
            }
            let (block, slot) = location.ok_or("dense locator lost a key")?;
            let decoded = block_cache.entry(block).or_insert_with(|| {
                decode_block(&read_sealed(&pack, blocks[block].offset, sealer, cap).expect("block"))
            });
            decoded.get(slot).map(|(_, v)| v.clone())
        } else {
            let b = blocks.partition_point(|blk| blk.first_key.as_slice() <= key.as_slice()) - 1;
            let decoded = block_cache.entry(b).or_insert_with(|| {
                decode_block(&read_sealed(&pack, blocks[b].offset, sealer, cap).expect("block"))
            });
            decoded
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
        };
        if found.as_ref() != Some(value) {
            return Err("v2 point get differs".into());
        }
        if block_cache.len() > 256 {
            block_cache.clear();
        }
    }
    Ok(format!(
        "all {} records equal; {gets} point gets equal",
        records.len()
    ))
}

// ---------------------------------------------------------------------------
// Measurement and report.

#[derive(Default)]
struct Measure {
    apparent: u64,
    allocated: u64,
    parts: Vec<(String, u64)>,
    keys: u64,
    write_secs: f64,
    verified: String,
}

fn measure_dir(root: &Path) -> Result<Measure> {
    let mut measure = Measure::default();
    let mut parts: HashMap<String, u64> = HashMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                measure.apparent += meta.len();
                measure.allocated += meta.blocks() * 512;
                let rel = entry.path().strip_prefix(root)?.to_path_buf();
                let top = rel
                    .components()
                    .next()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .unwrap_or_default();
                *parts.entry(top).or_default() += meta.blocks() * 512;
            }
        }
    }
    let mut parts: Vec<_> = parts
        .into_iter()
        .filter(|(_, b)| *b >= 1024 * 1024)
        .collect();
    parts.sort_by(|a, b| b.1.cmp(&a.1));
    measure.parts = parts;
    Ok(measure)
}

struct Row {
    name: String,
    logical: u64,
    measure: Measure,
}

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true") {
            return Err("this spike runs only in GitHub Actions".into());
        }
        let parent = fs::canonicalize(std::env::var("RUNNER_TEMP")?)?;
        let path = parent.join(format!("disk-size-spike-{}", uuid::Uuid::new_v4()));
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Only this create-new CI directory is owned by the spike.
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn clww_key_bytes(stream: &Stream) -> (f64, f64) {
    // `lastdb_range_crypto`: one 16-byte block per input bit, stored as hex.
    let mut today = 0u64;
    let mut clww = 0u64;
    let mut tips = 0u64;
    for item in &stream.items {
        if let Item::Tip { range, .. } = item {
            tips += 1;
            let prefix = 3 + 43 + 1 + 22;
            let r = range.as_ref().map_or(0, |r| r.len());
            today += (prefix + if r > 0 { 1 + r * 8 } else { 0 }) as u64;
            clww += (prefix + if r > 0 { 1 + r * 8 * 16 * 2 } else { 0 }) as u64;
        }
    }
    (today as f64 / tips as f64, clww as f64 / tips as f64)
}

fn main() -> Result<()> {
    // lint:fn-size-ok spike driver kept linear so the measured steps read in order.
    let scratch = Scratch::new()?;
    let today = build_stream(&StreamOptions { proteins: false });
    let proteins = build_stream(&StreamOptions { proteins: true });
    println!(
        "seed: {} atoms, {:.1} MiB logical, {} stored items (protein seed: {} atoms, {:.1} MiB)",
        today.atoms,
        today.logical_atom_bytes as f64 / MIB,
        today.items.len(),
        proteins.atoms,
        proteins.logical_atom_bytes as f64 / MIB,
    );
    let replaced: Vec<&str> = SHAPES
        .iter()
        .filter(|shape| shape.replaced_by_protein)
        .map(|shape| shape.name)
        .collect();
    println!("protein members replace: {}", replaced.join(", "));
    let mut rows = Vec::new();
    let v1 = [
        (
            "A v1 today",
            V1Options {
                raw_seal: false,
                raw_range: false,
                implied_counter: false,
                bounded_bookkeeping: false,
            },
        ),
        (
            "B v1 + ENB binary seal",
            V1Options {
                raw_seal: true,
                raw_range: false,
                implied_counter: false,
                bounded_bookkeeping: false,
            },
        ),
        (
            "C v1 + B + bounded idem/event",
            V1Options {
                raw_seal: true,
                raw_range: false,
                implied_counter: false,
                bounded_bookkeeping: true,
            },
        ),
        (
            "D v1 + C + raw range",
            V1Options {
                raw_seal: true,
                raw_range: true,
                implied_counter: false,
                bounded_bookkeeping: true,
            },
        ),
        (
            "E v1 + D + implied counter",
            V1Options {
                raw_seal: true,
                raw_range: true,
                implied_counter: true,
                bounded_bookkeeping: true,
            },
        ),
    ];
    for (n, (name, options)) in v1.iter().enumerate() {
        // The home builder creates this directory itself.
        let root = scratch.0.join(format!("v1-{n}"));
        let measure = write_v1(&root, &today, options)?;
        println!("{name}: {:.1} MiB", measure.allocated as f64 / MIB);
        rows.push(Row {
            name: name.to_string(),
            logical: today.logical_atom_bytes,
            measure,
        });
        fs::remove_dir_all(&root)?;
    }
    let v2 = [
        (
            "F v2 dense locator, 16 KiB blocks, zstd 3",
            V2Options {
                block_bytes: 16 * 1024,
                zstd_level: 3,
                dense_locator: true,
            },
            false,
        ),
        (
            "G v2 sparse index, 16 KiB blocks, zstd 3",
            V2Options {
                block_bytes: 16 * 1024,
                zstd_level: 3,
                dense_locator: false,
            },
            false,
        ),
        (
            "H v2 sparse index, 64 KiB blocks, zstd 3",
            V2Options {
                block_bytes: 64 * 1024,
                zstd_level: 3,
                dense_locator: false,
            },
            false,
        ),
        (
            "I v2 sparse index, 64 KiB blocks, zstd 9",
            V2Options {
                block_bytes: 64 * 1024,
                zstd_level: 9,
                dense_locator: false,
            },
            false,
        ),
        (
            "J v2 sparse 16 KiB + brain proteins",
            V2Options {
                block_bytes: 16 * 1024,
                zstd_level: 3,
                dense_locator: false,
            },
            true,
        ),
        (
            "K v2 dense 16 KiB + brain proteins",
            V2Options {
                block_bytes: 16 * 1024,
                zstd_level: 3,
                dense_locator: true,
            },
            true,
        ),
    ];
    for (n, (name, options, with_proteins)) in v2.iter().enumerate() {
        let stream = if *with_proteins { &proteins } else { &today };
        let root = scratch.0.join(format!("v2-{n}"));
        let measure = write_v2(&root, stream, *options)?;
        println!("{name}: {:.1} MiB", measure.allocated as f64 / MIB);
        rows.push(Row {
            name: name.to_string(),
            logical: stream.logical_atom_bytes,
            measure,
        });
        fs::remove_dir_all(&root)?;
    }
    let (ope_key, clww_key) = clww_key_bytes(&today);
    let base = today.logical_atom_bytes as f64;
    println!();
    println!("| Variant | Disk MiB | Ratio to 84 MiB seed | Ratio to own atoms | Stored keys | Largest parts | Write s | Read-back |");
    println!("|---|---|---|---|---|---|---|---|");
    for row in &rows {
        let parts: Vec<String> = row
            .measure
            .parts
            .iter()
            .take(3)
            .map(|(p, b)| format!("{p} {:.0}", *b as f64 / MIB))
            .collect();
        println!(
            "| {} | {:.1} | {:.2}x | {:.2}x | {} | {} | {:.1} | {} |",
            row.name,
            row.measure.allocated as f64 / MIB,
            row.measure.allocated as f64 / base,
            row.measure.allocated as f64 / row.logical as f64,
            row.measure.keys,
            parts.join(", "),
            row.measure.write_secs,
            row.measure.verified,
        );
    }
    println!();
    println!(
        "Average tip key: {ope_key:.0} B with today's OPE range; {clww_key:.0} B with the lastdb_range_crypto CLWW candidate (not written)."
    );
    println!("Primary on 2026-10-24 for calibration: 865.4 MiB data for 84.0 MiB atoms (10.30x).");
    Ok(())
}
