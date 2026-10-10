//! The identities file: the names of the dropped schemas, one per line.

use std::collections::BTreeSet;
use std::path::Path;

use fold_db::hex::sha256_hex;

use super::ReapError;

/// The dropped names Tom approved.
///
/// A line is a hash identity or a friendly row such as
/// `lastgit/LastgitCiStatus`. The catalog may spell a friendly row with or
/// without the owner part, so both spellings are names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Identities {
    /// The lines of the file, in file order, without duplicates.
    pub listed: Vec<String>,
    /// Every spelling of every listed name.
    pub spellings: BTreeSet<String>,
    /// SHA-256 of the file text.
    pub sha256: String,
}

impl Identities {
    /// Every spelling of one listed name.
    pub(crate) fn spellings_of(line: &str) -> Vec<String> {
        let mut out = vec![line.to_string()];
        if let Some((_, tail)) = line.rsplit_once('/') {
            if !tail.is_empty() {
                out.push(tail.to_string());
            }
        }
        out
    }

    /// Listed names for which no spelling is in `found`.
    pub(crate) fn listed_without(&self, found: &BTreeSet<String>) -> Vec<String> {
        self.listed
            .iter()
            .filter(|line| {
                !Self::spellings_of(line)
                    .iter()
                    .any(|spelling| found.contains(spelling))
            })
            .cloned()
            .collect()
    }
}

/// Parse the text of an identities file.
///
/// A blank line and a line that starts with `#` are skipped. A name with
/// white space or a NUL byte is an error: no schema has such a name, and the
/// file is then not the file Tom approved.
pub(crate) fn parse(text: &str) -> Result<Identities, ReapError> {
    let mut listed: Vec<String> = Vec::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.chars().any(|c| c.is_whitespace() || c == '\0') {
            return Err(ReapError::Refused(format!(
                "identities file: a name has white space or a NUL byte: {line:?}"
            )));
        }
        if !listed.iter().any(|known| known == line) {
            listed.push(line.to_string());
        }
    }
    if listed.is_empty() {
        return Err(ReapError::Refused(
            "identities file lists no name".to_string(),
        ));
    }
    let spellings = listed
        .iter()
        .flat_map(|line| Identities::spellings_of(line))
        .collect();
    Ok(Identities {
        listed,
        spellings,
        sha256: sha256_hex(text.as_bytes()),
    })
}

/// Read and parse the identities file at `path`.
pub(crate) fn load(path: &Path) -> Result<Identities, ReapError> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        ReapError::Refused(format!("read identities file {}: {error}", path.display()))
    })?;
    parse(&text)
}
