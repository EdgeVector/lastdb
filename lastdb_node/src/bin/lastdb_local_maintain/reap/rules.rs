//! The rule set of one collection and its text form (contract v1).
//!
//! The planner counts with this matcher and writes the rules from this same
//! object. The engine matches with its own code. A bug in a rule then shows as
//! a count that differs between the two.

use std::collections::BTreeSet;

use fold_db::hex::{hex_decode, hex_lower};
use sha2::{Digest, Sha256};

/// The 128-bit hash of a key, for the exact-key files.
///
/// A hash collision can only add a key to a count. It never hides a key.
pub(crate) fn key_hash(key: &[u8]) -> u128 {
    let digest = Sha256::digest(key);
    let mut head = [0u8; 16];
    head.copy_from_slice(&digest[..16]);
    u128::from_be_bytes(head)
}

/// One `exact_file` directive: the path and the hash of every key in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExactFile {
    /// Path relative to the plan directory.
    pub path: String,
    /// Sorted, without duplicates.
    pub hashes: Vec<u128>,
}

/// Prefix rules, exact keys and exact-key files of one collection.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RuleSet {
    prefixes: Vec<Vec<u8>>,
    exact: BTreeSet<Vec<u8>>,
    exact_files: Vec<ExactFile>,
}

/// The header lines of a rules file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RulesHeader {
    pub collection: String,
    pub expect_keys: Option<u64>,
    pub expect_bytes: Option<u64>,
}

impl RuleSet {
    pub(crate) fn add_prefix(&mut self, prefix: &[u8]) {
        self.prefixes.push(prefix.to_vec());
    }

    pub(crate) fn add_exact(&mut self, key: &[u8]) {
        self.exact.insert(key.to_vec());
    }

    pub(crate) fn add_exact_file(&mut self, path: &str, mut hashes: Vec<u128>) {
        hashes.sort_unstable();
        hashes.dedup();
        self.exact_files.push(ExactFile {
            path: path.to_string(),
            hashes,
        });
    }

    /// Sort the prefixes and drop each prefix that a shorter prefix covers.
    ///
    /// After this call no prefix is the prefix of another. The nearest smaller
    /// prefix is then the only one that can match a key.
    pub(crate) fn finish(&mut self) {
        self.prefixes.sort();
        self.prefixes.dedup();
        let mut kept: Vec<Vec<u8>> = Vec::with_capacity(self.prefixes.len());
        for prefix in self.prefixes.drain(..) {
            if kept.last().is_some_and(|last| prefix.starts_with(last)) {
                continue;
            }
            kept.push(prefix);
        }
        self.prefixes = kept;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.prefixes.is_empty() && self.exact.is_empty() && self.exact_files.is_empty()
    }

    pub(crate) fn rule_count(&self) -> usize {
        self.prefixes.len()
            + self.exact.len()
            + self
                .exact_files
                .iter()
                .map(|file| file.hashes.len())
                .sum::<usize>()
    }

    /// True when any rule drops `key`.
    pub(crate) fn matches(&self, key: &[u8]) -> bool {
        let at = self
            .prefixes
            .partition_point(|prefix| prefix.as_slice() <= key);
        if at > 0 && key.starts_with(&self.prefixes[at - 1]) {
            return true;
        }
        if self.exact.contains(key) {
            return true;
        }
        if self.exact_files.is_empty() {
            return false;
        }
        let hash = key_hash(key);
        self.exact_files
            .iter()
            .any(|file| file.hashes.binary_search(&hash).is_ok())
    }

    /// The text of the rules file.
    pub(crate) fn render(&self, collection: &str, expect_keys: u64, expect_bytes: u64) -> String {
        let mut out = String::new();
        out.push_str("# reap rules, contract v1. Written by lastdb_local_maintain reap plan.\n");
        out.push_str("version 1\n");
        out.push_str(&format!("collection {collection}\n"));
        out.push_str(&format!("expect_keys {expect_keys}\n"));
        out.push_str(&format!("expect_bytes {expect_bytes}\n"));
        for prefix in &self.prefixes {
            out.push_str(&format!("prefix {}\n", hex_lower(prefix)));
        }
        for key in &self.exact {
            out.push_str(&format!("exact {}\n", hex_lower(key)));
        }
        for file in &self.exact_files {
            out.push_str(&format!("exact_file {}\n", file.path));
        }
        out
    }

    /// Read a rules file. `read_hashes` returns the hashes of one exact file.
    ///
    /// This is the reader of the grammar in the contract. The planner uses it
    /// to prove that the file it wrote says what the matcher says.
    pub(crate) fn parse(
        text: &str,
        mut read_hashes: impl FnMut(&str) -> Result<Vec<u128>, String>,
    ) -> Result<(Self, RulesHeader), String> {
        let mut set = Self::default();
        let mut version = None;
        let mut collection = None;
        let mut expect_keys = None;
        let mut expect_bytes = None;
        for line in text.lines() {
            let line = line.trim_end();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (word, arg) = line
                .split_once(' ')
                .ok_or_else(|| format!("rules line has no argument: {line:?}"))?;
            match word {
                "version" => version = Some(arg.to_string()),
                "collection" => collection = Some(arg.to_string()),
                "expect_keys" => expect_keys = Some(parse_u64(arg)?),
                "expect_bytes" => expect_bytes = Some(parse_u64(arg)?),
                "prefix" => set.add_prefix(&decode_hex(arg)?),
                "exact" => set.add_exact(&decode_hex(arg)?),
                "exact_file" => set.add_exact_file(arg, read_hashes(arg)?),
                other => return Err(format!("unknown rules directive: {other}")),
            }
        }
        if version.as_deref() != Some("1") {
            return Err("rules file has no `version 1` line".to_string());
        }
        let collection = collection.ok_or("rules file has no collection line")?;
        set.finish();
        Ok((
            set,
            RulesHeader {
                collection,
                expect_keys,
                expect_bytes,
            },
        ))
    }
}

fn parse_u64(text: &str) -> Result<u64, String> {
    text.parse::<u64>()
        .map_err(|error| format!("bad number {text:?}: {error}"))
}

fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    if text.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(format!("hex must be lowercase: {text:?}"));
    }
    hex_decode(text).ok_or_else(|| format!("bad hex: {text:?}"))
}

/// Hashes of the keys in an exact-key file (one lowercase hex key per line).
pub(crate) fn hashes_of_exact_text(text: &str) -> Result<Vec<u128>, String> {
    let mut hashes = Vec::new();
    for line in text.lines() {
        let line = line.trim_end();
        if line.is_empty() {
            continue;
        }
        hashes.push(key_hash(&decode_hex(line)?));
    }
    hashes.sort_unstable();
    hashes.dedup();
    Ok(hashes)
}
