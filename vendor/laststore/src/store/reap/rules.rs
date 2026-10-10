//! Parse one rules file.
//!
//! The file is UTF-8 text with LF line ends. A blank line and a line that
//! starts with `#` are ignored. Each other line holds one directive, one
//! space, and one value:
//!
//! ```text
//! version 1
//! collection <name>
//! expect_keys <N>
//! expect_bytes <N>
//! prefix <hex>
//! exact <hex>
//! exact_file <relative-path>
//! ```
//!
//! `<hex>` is lowercase hex of the raw key bytes. The parser decodes it and
//! never reads it as text.

use super::ReapError;

/// The directives of one rules file, before the key files are read.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ParsedRules {
    pub collection: String,
    pub expect_keys: Option<u64>,
    pub expect_bytes: Option<u64>,
    pub prefixes: Vec<Vec<u8>>,
    pub exact: Vec<Vec<u8>>,
    pub exact_files: Vec<String>,
}

/// Decode lowercase hex. An empty string, odd length, or any other
/// character is an error.
pub(super) fn decode_lower_hex(text: &str) -> Result<Vec<u8>, String> {
    if text.is_empty() {
        return Err("hex value is empty".to_string());
    }
    if text.len() % 2 != 0 {
        return Err("hex value has an odd length".to_string());
    }
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
    text.as_bytes()
        .chunks(2)
        .map(|pair| match (nibble(pair[0]), nibble(pair[1])) {
            (Some(high), Some(low)) => Ok((high << 4) | low),
            _ => Err("hex value has a character outside 0-9 a-f".to_string()),
        })
        .collect()
}

fn parse_count(value: &str) -> Result<u64, String> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("`{value}` is not a decimal count"));
    }
    value
        .parse::<u64>()
        .map_err(|_| format!("`{value}` does not fit in 64 bits"))
}

/// A relative path that stays under the plan directory.
fn check_relative_path(value: &str) -> Result<(), String> {
    if value.is_empty() || value.contains('\0') {
        return Err("exact_file path is empty or holds a NUL byte".to_string());
    }
    let path = std::path::Path::new(value);
    let plain = path
        .components()
        .all(|part| matches!(part, std::path::Component::Normal(_)));
    if path.is_absolute() || !plain {
        return Err(format!(
            "exact_file path `{value}` must be relative with no `..`"
        ));
    }
    Ok(())
}

fn set_once<T: PartialEq>(slot: &mut Option<T>, value: T, name: &str) -> Result<(), String> {
    match slot {
        Some(existing) if *existing != value => {
            Err(format!("`{name}` is given twice with different values"))
        }
        _ => {
            *slot = Some(value);
            Ok(())
        }
    }
}

struct Draft {
    version: Option<u64>,
    collection: Option<String>,
    parsed: ParsedRules,
}

fn apply_directive(draft: &mut Draft, word: &str, value: &str) -> Result<(), String> {
    if word != "version" && draft.version.is_none() {
        return Err("`version 1` must be the first directive".to_string());
    }
    match word {
        "version" => {
            if parse_count(value)? != 1 {
                return Err(format!("version `{value}` is not supported"));
            }
            set_once(&mut draft.version, 1, "version")
        }
        "collection" => {
            if value.is_empty() {
                return Err("collection name is empty".to_string());
            }
            set_once(&mut draft.collection, value.to_string(), "collection")
        }
        "expect_keys" => set_once(
            &mut draft.parsed.expect_keys,
            parse_count(value)?,
            "expect_keys",
        ),
        "expect_bytes" => set_once(
            &mut draft.parsed.expect_bytes,
            parse_count(value)?,
            "expect_bytes",
        ),
        "prefix" => {
            draft.parsed.prefixes.push(decode_lower_hex(value)?);
            Ok(())
        }
        "exact" => {
            draft.parsed.exact.push(decode_lower_hex(value)?);
            Ok(())
        }
        "exact_file" => {
            check_relative_path(value)?;
            draft.parsed.exact_files.push(value.to_string());
            Ok(())
        }
        other => Err(format!("unknown directive `{other}`")),
    }
}

/// Parse the text of `rules/<stem>.rules`.
///
/// `label` names the file in the error text. The `collection` directive must
/// exist and must equal `stem`.
pub(super) fn parse_rules(text: &str, stem: &str, label: &str) -> Result<ParsedRules, ReapError> {
    let refuse = |line: usize, reason: String| -> ReapError {
        ReapError::Refused(format!("{label} line {line}: {reason}"))
    };
    if text.contains('\r') {
        return Err(ReapError::Refused(format!(
            "{label}: a CR byte is present; use LF line ends"
        )));
    }
    let mut draft = Draft {
        version: None,
        collection: None,
        parsed: ParsedRules::default(),
    };
    for (index, line) in text.split('\n').enumerate() {
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let (word, value) = line.split_once(' ').unwrap_or((line, ""));
        let padded = line.starts_with(char::is_whitespace) || line.ends_with(char::is_whitespace);
        if padded || value.contains(char::is_whitespace) {
            return Err(refuse(
                index + 1,
                "use one space between the directive and the value, and no other space".to_string(),
            ));
        }
        apply_directive(&mut draft, word, value).map_err(|reason| refuse(index + 1, reason))?;
    }
    finish(draft, stem, label)
}

fn finish(draft: Draft, stem: &str, label: &str) -> Result<ParsedRules, ReapError> {
    if draft.version.is_none() {
        return Err(ReapError::Refused(format!(
            "{label}: `version 1` is missing"
        )));
    }
    let Some(collection) = draft.collection else {
        return Err(ReapError::Refused(format!(
            "{label}: the `collection` directive is missing"
        )));
    };
    if collection != stem {
        return Err(ReapError::Refused(format!(
            "{label}: collection `{collection}` differs from the file stem `{stem}`"
        )));
    }
    let mut parsed = draft.parsed;
    parsed.collection = collection;
    Ok(parsed)
}
