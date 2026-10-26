//! The values of the compact value phase: node-shaped JSON, random JSON
//! trees, texts near JSON, timestamps, and other bytes. All are
//! deterministic for one seed.

use crate::gen::Rng;
use laststore::disk_run::value::{COMPACT_JSON, KEYS};
use serde_json::{json, Map, Value};

const MUTATIONS: u64 = 6_000;
const OTHER: u64 = 3_000;
/// The largest value that can take the compact form.
const COMPACT_INPUT: usize = 1 << 20;
const WORDS: [&str; 14] = [
    "atom",
    "schema",
    "brain",
    "kanban",
    "note",
    "décision",
    "naïve",
    "日本語",
    "tab\there",
    "quote\"d",
    "back\\slash",
    "line\nbreak",
    "emoji 🙂",
    "€uro",
];

/// A record as the node writes it, as `serde_json::to_vec` text: an atom
/// row, a tip, a molecule entry, or a mutation record.
pub(crate) fn node_value(rng: &mut Rng, n: u64) -> Vec<u8> {
    let value = match n % 4 {
        0 => {
            let body = 8 + rng.below(40);
            json!({
                "content": {
                    "body": words(rng, body),
                    "tags": [words(rng, 1), words(rng, 1)],
                    "title": words(rng, 4),
                    "updated": time(rng),
                },
                "created_at": time(rng),
                "metadata": null,
                "source_file_name": null,
                "source_schema_name": hex(rng, 32),
                "uuid": uuid(rng),
            })
        }
        1 => {
            let previous = if rng.below(2) == 0 {
                Value::Null
            } else {
                Value::from(uuid(rng))
            };
            json!({
                "atom_uuid": uuid(rng),
                "logical_counter": rng.below(1 << 40),
                "mutation_uuid": uuid(rng),
                "prev_tip_id": previous,
                "signature": hex(rng, 64),
                "signature_version": 2,
                "writer_pubkey": hex(rng, 32),
                "written_at": time(rng),
            })
        }
        2 => json!({
            "field_key": words(rng, 1),
            "hash": hex(rng, 32),
            "molecule_uuid": uuid(rng),
            "range": time(rng),
            "version": rng.below(100),
        }),
        _ => json!({
            "background_tasks_drained": rng.below(2) == 0,
            "committed_at_ms": 1_761_000_000_000 + rng.below(1 << 32),
            "mutation_id": uuid(rng),
            "operation": "update",
            "refs": [{"src": uuid(rng), "sz": rng.below(1 << 20)}],
            "schema": hex(rng, 32),
        }),
    };
    text(&value)
}

/// A random JSON value with every element form, nested up to six deep.
pub(crate) fn tree(rng: &mut Rng, depth: u32) -> Value {
    let forms = if depth < 6 { 9 } else { 7 };
    match rng.below(forms) {
        0 => Value::Null,
        1 => Value::Bool(rng.below(2) == 0),
        2 => Value::from(rng.next_u64() >> rng.below(64)),
        3 => Value::from(-1 - (rng.next_u64() >> (1 + rng.below(63))) as i64),
        4 => Value::from(rng.below(1 << 53) as f64 * 10f64.powi(rng.below(41) as i32 - 20)),
        5 => {
            let count = rng.below(6);
            Value::from(words(rng, count))
        }
        6 => Value::from(special_string(rng)),
        7 => {
            let count = rng.below(5);
            Value::Array((0..count).map(|_| tree(rng, depth + 1)).collect())
        }
        _ => {
            let mut object = Map::new();
            for _ in 0..rng.below(6) {
                let key = if rng.below(2) == 0 {
                    KEYS[rng.below(KEYS.len() as u64) as usize].to_string()
                } else {
                    words(rng, 1)
                };
                let value = tree(rng, depth + 1);
                object.insert(key, value);
            }
            Value::Object(object)
        }
    }
}

/// Strings near the typed forms: UUID, hex, and timestamp texts, valid or
/// not.
fn special_string(rng: &mut Rng) -> String {
    match rng.below(8) {
        0 => uuid(rng),
        1 => uuid(rng).to_uppercase(),
        2 => uuid(rng).replace('-', ""),
        3 => {
            let bytes = 1 + rng.below(40) as usize;
            hex(rng, bytes)
        }
        4 => hex(rng, 8).to_uppercase(),
        5 => {
            let mut odd = hex(rng, 8);
            odd.pop();
            odd
        }
        6 => time(rng),
        _ => time(rng).replace('T', " "),
    }
}

/// Edge texts of each element form, node values made not compact or not
/// JSON, and texts at the compact input limit.
pub(crate) fn near_json(rng: &mut Rng) -> Vec<Vec<u8>> {
    let mut out = Vec::from(
        [
            "{}", "[]", "[{}]", "{\"a\":[]}", "null", "\"text\"", "12", "{not json}", "{\"a\"}",
            "{\"a\":}", "{,}", "[,]", "[1,]", "{\"a\":1,}", "{\"v\":01}", "{\"v\":-0}",
            "{\"v\":1e400}", "{\"v\":-}", "{\"v\":1-2}", "{\"v\":+1}", "{\"v\":.5}",
            "{\"v\":1,\"v\":2}", "[\"\\u00E9\",\"\\x\",\"\\\\\"]", "[\"a\\\"]",
            "{\"atom_uuid\" :1}", "[1 ,2]", "[true,fals]", "[nul]", "[\"\"]", "{\"\":\"\"}",
            "[18446744073709551615,18446744073709551616,-18446744073709551615,-18446744073709551616]",
        ]
        .map(|edge| edge.as_bytes().to_vec()),
    );
    out.push(b"[\"\xff\xfe\"]".to_vec());
    out.push(b"{\"k\":\"\x00\x01\"}".to_vec());
    for depth in [127, 128, 129, 130, 200] {
        out.push(format!("{}{}", "[".repeat(depth), "]".repeat(depth)).into_bytes());
        out.push(format!("{}1{}", "{\"a\":".repeat(depth), "}".repeat(depth)).into_bytes());
    }
    for n in 0..MUTATIONS {
        let mut bytes = node_value(rng, n);
        let at = rng.below(bytes.len() as u64) as usize;
        match n % 6 {
            0 => bytes.push(b'\n'),
            1 => bytes.truncate(at),
            2 => bytes[at] = rng.next_u64() as u8,
            3 => bytes.insert(at, b' '),
            4 => {
                let value: Value = serde_json::from_slice(&bytes).expect("a node value is JSON");
                bytes = serde_json::to_vec_pretty(&value).expect("a JSON value has a text");
            }
            _ => bytes.insert(at, b'"'),
        }
        out.push(bytes);
    }
    for size in [COMPACT_INPUT - 64, COMPACT_INPUT + 64] {
        let mut text = String::from("[");
        while text.len() < size - 64 {
            if text.len() > 1 {
                text.push(',');
            }
            let item = words(rng, 3);
            text.push_str(&serde_json::to_string(&item).expect("a string has a text"));
        }
        text.push(']');
        out.push(text.into_bytes());
    }
    out
}

/// Objects with timestamp texts: edge dates, texts that are not valid
/// timestamps, each fraction length, and valid dates over the whole range.
pub(crate) fn timestamps(rng: &mut Rng) -> Vec<Vec<u8>> {
    let mut texts = Vec::from(
        [
            "1970-01-01T00:00:00Z",
            "1969-12-31T23:59:59Z",
            "0000-01-01T00:00:00Z",
            "9999-12-31T23:59:59.999999999Z",
            "2000-02-29T12:00:00Z",
            "2024-02-29T00:00:00.5Z",
            "2100-02-28T23:59:59Z",
            "2026-10-25T12:34:56.000000001Z",
            "2026-10-25T12:34:56.000Z",
            "2023-02-29T00:00:00Z",
            "2100-02-29T00:00:00Z",
            "2026-13-01T00:00:00Z",
            "2026-00-10T00:00:00Z",
            "2026-10-32T00:00:00Z",
            "2026-10-25T24:00:00Z",
            "2026-10-25T23:60:00Z",
            "2026-10-25T23:59:60Z",
            "2026-10-25T12:34:56.1234567890Z",
            "2026-10-25T12:34:56.Z",
            "2026-10-25t12:34:56Z",
            "2026-10-25T12:34:56z",
            "2026-10-25T12:34:56+00:00",
            "2026-10-25 12:34:56Z",
            "+2026-10-25T12:34:56Z",
            "2026-1-25T12:34:56Z",
            "-001-10-25T12:34:56Z",
            "2026-10-25T12:34:5aZ",
        ]
        .map(String::from),
    );
    for digits in 1..=9usize {
        let fraction = rng.below(10u64.pow(digits as u32));
        texts.push(format!("2026-10-25T01:02:03.{fraction:0digits$}Z"));
    }
    for _ in 0..2_000 {
        let year = rng.below(10_000) as i64;
        let month = 1 + rng.below(12);
        let day = 1 + rng.below(days_in_month(year, month));
        let (hour, minute, second) = (rng.below(24), rng.below(60), rng.below(60));
        texts.push(format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z"
        ));
    }
    texts
        .iter()
        .map(|stamp| text(&json!({ "logical_counter": 1, "written_at": stamp })))
        .collect()
}

/// Bytes that are not JSON: tag bytes, an envelope prefix, a stored compact
/// form given as a value, random bytes, and plain text.
pub(crate) fn other_bytes(rng: &mut Rng) -> Vec<Vec<u8>> {
    let mut out = vec![
        Vec::new(),
        vec![0],
        vec![1],
        vec![0, 1, 2],
        vec![COMPACT_JSON, 11, 0],
        b"ENC:".to_vec(),
    ];
    for n in 0..OTHER {
        let len = rng.below(400) as usize;
        let mut bytes: Vec<u8> = (0..len).map(|_| rng.next_u64() as u8).collect();
        if let Some(first) = bytes.first_mut() {
            let kept = *first;
            *first = [0, 1, b'{', b'[', b'E', kept][(n % 6) as usize];
        }
        out.push(bytes);
    }
    out.push(words(rng, 50).into_bytes());
    out
}

pub(crate) fn text(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("a JSON value has a text")
}

fn words(rng: &mut Rng, count: u64) -> String {
    let mut out = String::new();
    for at in 0..count {
        if at > 0 {
            out.push(' ');
        }
        out.push_str(WORDS[rng.below(WORDS.len() as u64) as usize]);
    }
    out
}

/// A valid UTC timestamp text with 0, 3, 6, or 9 fraction digits.
fn time(rng: &mut Rng) -> String {
    let (year, month, day) = (1970 + rng.below(130), 1 + rng.below(12), 1 + rng.below(28));
    let (hour, minute, second) = (rng.below(24), rng.below(60), rng.below(60));
    let digits = 3 * rng.below(4) as usize;
    let fraction = rng.below(10u64.pow(digits as u32));
    let fraction = if digits == 0 {
        String::new()
    } else {
        format!(".{fraction:0digits$}")
    };
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{fraction}Z")
}

/// Lowercase hex text of `bytes` random bytes.
fn hex(rng: &mut Rng, bytes: usize) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    (0..2 * bytes)
        .map(|_| char::from(DIGITS[rng.below(16) as usize]))
        .collect()
}

fn uuid(rng: &mut Rng) -> String {
    let digits = hex(rng, 16);
    format!(
        "{}-{}-{}-{}-{}",
        &digits[..8],
        &digits[8..12],
        &digits[12..16],
        &digits[16..20],
        &digits[20..]
    )
}

fn days_in_month(year: i64, month: u64) -> u64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}
