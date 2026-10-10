//! A read-only check of the newest segment of a group, before the group loads.
//!
//! A load of a plain group cuts the newest segment back to its last whole
//! record and syncs the file. That cut is a write. It also drops every record
//! after the first bad one, so one bad sector can cost the rest of the
//! segment. The reap must not do that, least of all in a count pass.
//!
//! So the reap walks the newest segment first, with the same parser as the
//! load. It refuses a group that the load would cut. No byte changes. Older
//! segments need no walk: a load never cuts them, and it stops with an error
//! on a bad one.

use super::{group_label, ReapError};
use crate::segfmt;
use crate::sorted;
use crate::store::{LastStore, ShardHandle, ShardKey};
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

/// A newest segment that does not hold only whole records.
#[derive(Debug)]
pub(super) struct TornSegment {
    /// File name of the segment.
    pub file: String,
    /// Offset of the first record that is not whole.
    pub offset: usize,
    /// Size of the file.
    pub len: usize,
    /// The parser error, when the record is bad and not cut short.
    pub cause: Option<String>,
}

impl TornSegment {
    /// Text for the operator. `label` names the group.
    pub(super) fn describe(&self, label: &str) -> String {
        let place = format!(
            "{} at offset {} of {} bytes",
            self.file, self.offset, self.len
        );
        let (what, effect) = match &self.cause {
            None => (
                format!("ends in a torn record, {place}"),
                "A load would cut the file there and drop the bytes after it.".to_string(),
            ),
            Some(cause) => (
                format!("holds a bad record ({cause}), {place}"),
                "A load would stop with an error.".to_string(),
            ),
        };
        format!(
            "{label}: {} {what}. {effect} The reap changed no byte. \
             Repair the group, then run the reap again.",
            self.file
        )
    }
}

/// The highest sequence among the `*.seg` files of a group directory.
///
/// The rule is the one of the load: the extension is `seg` and the stem is a
/// number. A directory that does not exist has no segment.
pub(super) fn newest_sequence(dir: &Path) -> Result<Option<u64>, ReapError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut newest = None;
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|x| x.to_str()) != Some("seg") {
            continue;
        }
        let sequence = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u64>().ok());
        newest = newest.max(sequence);
    }
    Ok(newest)
}

/// The newest plain segment of the group, when it is not whole.
///
/// This reads the file and writes nothing. A sorted segment is not cut by a
/// load, so it passes here. The group is refused later, as a sorted group.
pub(super) fn torn_tail(dir: &Path) -> Result<Option<TornSegment>, ReapError> {
    let Some(sequence) = newest_sequence(dir)? else {
        return Ok(None);
    };
    let file = format!("{sequence:010}.seg");
    let path = dir.join(&file);
    if sorted::Segment::recognizes(&path)? {
        return Ok(None);
    }
    let data = fs::read(&path)?;
    let mut offset = 0usize;
    while offset < data.len() {
        let cause = match segfmt::parse_at(&data, offset) {
            Ok(Some(record)) => {
                offset += record.raw_len;
                continue;
            }
            Ok(None) => None,
            Err(error) => Some(error.to_string()),
        };
        return Ok(Some(TornSegment {
            file,
            offset,
            len: data.len(),
            cause,
        }));
    }
    Ok(None)
}

impl LastStore {
    /// Load a group for the reap, after a check that the load cuts nothing.
    ///
    /// `torn` makes the error for a group that fails the check.
    fn reap_open_group(
        &self,
        key: ShardKey,
        torn: fn(String) -> ReapError,
    ) -> Result<ShardHandle, ReapError> {
        let dir = self.handle_dir(&key.0, key.1, key.2);
        if let Some(found) = torn_tail(&dir)? {
            return Err(torn(found.describe(&group_label(&key.0, key.1, key.2))));
        }
        Ok(self.open_group_unpublished(key)?)
    }

    /// Load a group in the count pass. A torn tail is a refusal: exit 2.
    pub(super) fn reap_open_for_count(&self, key: ShardKey) -> Result<ShardHandle, ReapError> {
        self.reap_open_group(key, ReapError::Refused)
    }

    /// Load a group in the apply pass, before or after its rewrite. The count
    /// pass saw the group whole, so a torn tail means that the group changed
    /// or that the rewrite is wrong: a gate mismatch, exit 4.
    pub(super) fn reap_open_for_apply(&self, key: ShardKey) -> Result<ShardHandle, ReapError> {
        self.reap_open_group(key, ReapError::GateMismatch)
    }
}
