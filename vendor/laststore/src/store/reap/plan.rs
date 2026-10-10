//! Load a plan directory.
//!
//! ```text
//! <plan-dir>/
//!   rules/<collection>.rules   one file for each collection that has rules
//!   exact/<collection>.keys    optional files of exact keys, one hex key a line
//! ```
//!
//! Other files in the plan directory are not read here.

use super::matcher::Matcher;
use super::rules::{decode_lower_hex, parse_rules};
use super::{ReapError, REAP_ALLOWED_COLLECTIONS};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// The rules of one collection, with the counts the reap must reach.
pub struct CollectionRules {
    /// Collection name. It equals the rules file stem.
    pub collection: String,
    /// Live keys the rules must match. `execute` needs it.
    pub expect_keys: Option<u64>,
    /// Sum of key length and value length the rules must match.
    pub expect_bytes: Option<u64>,
    pub(super) matcher: Matcher,
}

impl CollectionRules {
    /// The compiled rules.
    pub(crate) fn matcher(&self) -> &Matcher {
        &self.matcher
    }
}

/// The rules of every selected collection, in name order.
pub struct ReapPlan {
    /// One entry for each selected collection.
    pub collections: Vec<CollectionRules>,
}

/// A collection name from a rules file stem or from `--collections`.
fn check_collection_name(name: &str) -> Result<(), ReapError> {
    let plain = !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    if !plain {
        return Err(ReapError::Refused(format!(
            "collection name `{name}` is not a plain lowercase name"
        )));
    }
    check_allowed(name)
}

/// Refuse a collection that is not in the allow-list.
pub(super) fn check_allowed(name: &str) -> Result<(), ReapError> {
    if REAP_ALLOWED_COLLECTIONS.contains(&name) {
        Ok(())
    } else {
        Err(ReapError::Refused(format!(
            "collection `{name}` is not in the allow-list"
        )))
    }
}

/// Stems of the `*.rules` files under `rules/`, sorted.
fn rule_stems(rules_dir: &Path) -> Result<Vec<String>, ReapError> {
    let entries = fs::read_dir(rules_dir).map_err(|error| {
        ReapError::Refused(format!("cannot read {}: {error}", rules_dir.display()))
    })?;
    let mut stems = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("rules") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            return Err(ReapError::Refused(format!(
                "rules file name is not UTF-8: {}",
                path.display()
            )));
        };
        stems.push(stem.to_string());
    }
    stems.sort();
    Ok(stems)
}

/// Read a file of hex keys into `out`. One key a line.
/// A blank line and a line that starts with `#` are ignored.
fn read_key_file(path: &Path, label: &str, out: &mut HashSet<Vec<u8>>) -> Result<(), ReapError> {
    let file = File::open(path)
        .map_err(|error| ReapError::Refused(format!("cannot open {label}: {error}")))?;
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line =
            line.map_err(|error| ReapError::Refused(format!("cannot read {label}: {error}")))?;
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let key = decode_lower_hex(&line).map_err(|reason| {
            ReapError::Refused(format!("{label} line {}: {reason}", index + 1))
        })?;
        out.insert(key);
    }
    Ok(())
}

/// Resolve an `exact_file` path. It must be a regular file under `plan_dir`.
fn resolve_key_file(plan_dir: &Path, relative: &str) -> Result<PathBuf, ReapError> {
    let joined = plan_dir.join(relative);
    let real = fs::canonicalize(&joined).map_err(|error| {
        ReapError::Refused(format!("exact_file `{relative}` cannot be opened: {error}"))
    })?;
    let root = fs::canonicalize(plan_dir)?;
    if !real.starts_with(&root) || !real.is_file() {
        return Err(ReapError::Refused(format!(
            "exact_file `{relative}` is not a regular file under the plan directory"
        )));
    }
    Ok(real)
}

fn load_collection(plan_dir: &Path, name: &str) -> Result<CollectionRules, ReapError> {
    let path = plan_dir.join("rules").join(format!("{name}.rules"));
    let label = format!("rules/{name}.rules");
    let bytes = fs::read(&path).map_err(|error| {
        ReapError::Refused(format!(
            "no rules for collection `{name}` ({label}): {error}"
        ))
    })?;
    let text = String::from_utf8(bytes)
        .map_err(|_| ReapError::Refused(format!("{label} is not UTF-8")))?;
    let parsed = parse_rules(&text, name, &label)?;
    let mut exact: HashSet<Vec<u8>> = parsed.exact.into_iter().collect();
    for relative in &parsed.exact_files {
        let real = resolve_key_file(plan_dir, relative)?;
        read_key_file(&real, relative, &mut exact)?;
    }
    Ok(CollectionRules {
        collection: parsed.collection,
        expect_keys: parsed.expect_keys,
        expect_bytes: parsed.expect_bytes,
        matcher: Matcher::new(parsed.prefixes, exact),
    })
}

impl ReapPlan {
    /// Read the rules of the selected collections.
    ///
    /// `selected` of `None` selects every `rules/*.rules` file. A collection
    /// outside the allow-list and a selected collection with no rules file
    /// are refused. Rules files of collections that are not selected are not
    /// read.
    pub fn load(plan_dir: &Path, selected: Option<&[String]>) -> Result<Self, ReapError> {
        if !plan_dir.is_dir() {
            return Err(ReapError::Refused(format!(
                "plan directory is missing: {}",
                plan_dir.display()
            )));
        }
        let mut names = match selected {
            Some(names) => names.to_vec(),
            None => rule_stems(&plan_dir.join("rules"))?,
        };
        names.sort();
        names.dedup();
        if names.is_empty() {
            return Err(ReapError::Refused(
                "no collection is selected and no rules file exists".to_string(),
            ));
        }
        for name in &names {
            check_collection_name(name)?;
        }
        let mut collections = Vec::with_capacity(names.len());
        for name in &names {
            collections.push(load_collection(plan_dir, name)?);
        }
        Ok(Self { collections })
    }

    /// Check the allow-list again for every collection of the plan.
    pub(super) fn recheck_allow_list(&self) -> Result<(), ReapError> {
        for rules in &self.collections {
            check_allowed(&rules.collection)?;
        }
        Ok(())
    }
}
