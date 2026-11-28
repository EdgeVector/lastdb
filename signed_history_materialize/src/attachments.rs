//! External and inline file sections inside a shared-history record value.
//!
//! These keys use the same `lastdb:` prefix as the fields/alternates wrap, so
//! they cannot collide with user field names. Absent keys mean the write does
//! not touch that slot (Keep / no inline change in the decoded sense used by
//! materialize of a complete winner value).

use fold_db::record_value::{FileMarker, InlineFile, InlineWrite};
use serde_json::{json, Map, Value};

/// What a history value says about the external file slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileSection {
    /// The write does not touch the external file.
    Keep,
    /// The write clears the external file.
    Clear,
    /// The write sets the external file to this marker.
    Put(FileMarker),
}

pub(crate) const WRAP_FILE: &str = "lastdb:file";
pub(crate) const WRAP_INLINE: &str = "lastdb:inline_file";

/// Encode a file section into the wrap map, or omit it for Keep.
pub(crate) fn insert_file(map: &mut Map<String, Value>, file: &FileSection) -> Result<(), String> {
    match file {
        FileSection::Keep => Ok(()),
        FileSection::Clear => {
            map.insert(WRAP_FILE.to_owned(), json!({ "cleared": true }));
            Ok(())
        }
        FileSection::Put(marker) => {
            if !marker.has_valid_hash() {
                return Err("file marker hash is not 64 hex digits".into());
            }
            let mut section = Map::new();
            section.insert("cleared".into(), Value::Bool(false));
            section.insert(
                "content_sha256".into(),
                Value::String(marker.sha256.clone()),
            );
            section.insert("size".into(), json!(marker.size_bytes));
            if let Some(name) = &marker.name {
                section.insert("name".into(), Value::String(name.clone()));
            }
            if let Some(media_type) = &marker.media_type {
                section.insert("media_type".into(), Value::String(media_type.clone()));
            }
            map.insert(WRAP_FILE.to_owned(), Value::Object(section));
            Ok(())
        }
    }
}

/// Encode an inline write into the wrap map, or omit it when absent.
pub(crate) fn insert_inline(
    map: &mut Map<String, Value>,
    inline: Option<&InlineWrite>,
) -> Result<(), String> {
    let Some(write) = inline else {
        return Ok(());
    };
    match write {
        InlineWrite::Clear => {
            map.insert(WRAP_INLINE.to_owned(), json!({ "cleared": true }));
            Ok(())
        }
        InlineWrite::Put(file) => {
            if !file.is_well_formed() {
                return Err("inline file does not match its marker".into());
            }
            file.bytes()?;
            let marker = serde_json::to_value(&file.marker)
                .map_err(|error| format!("cannot encode inline marker: {error}"))?;
            map.insert(
                WRAP_INLINE.to_owned(),
                json!({
                    "cleared": false,
                    "marker": marker,
                    "data": file.data,
                }),
            );
            Ok(())
        }
    }
}

/// Decode the external file section from a wrap map (or a plain fields map).
pub(crate) fn take_file(map: &mut Map<String, Value>) -> Result<FileSection, String> {
    let Some(value) = map.remove(WRAP_FILE) else {
        return Ok(FileSection::Keep);
    };
    let Value::Object(section) = value else {
        return Err("lastdb:file is not a JSON object".into());
    };
    let cleared = section
        .get("cleared")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if cleared {
        let empty = section.keys().all(|key| key == "cleared");
        return if empty {
            Ok(FileSection::Clear)
        } else {
            Err("a cleared file section carries other fields".into())
        };
    }
    let sha256 = section
        .get("content_sha256")
        .and_then(Value::as_str)
        .ok_or("file section is missing content_sha256")?
        .to_owned();
    let size_bytes = section
        .get("size")
        .and_then(Value::as_u64)
        .ok_or("file section is missing size")?;
    let name = section
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let media_type = section
        .get("media_type")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let marker = FileMarker {
        sha256,
        size_bytes,
        name,
        media_type,
    };
    if !marker.has_valid_hash() {
        return Err("file marker hash is not 64 hex digits".into());
    }
    Ok(FileSection::Put(marker))
}

/// Decode the inline file section from a wrap map.
pub(crate) fn take_inline(map: &mut Map<String, Value>) -> Result<Option<InlineWrite>, String> {
    let Some(value) = map.remove(WRAP_INLINE) else {
        return Ok(None);
    };
    let Value::Object(section) = value else {
        return Err("lastdb:inline_file is not a JSON object".into());
    };
    let cleared = section
        .get("cleared")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if cleared {
        return Ok(Some(InlineWrite::Clear));
    }
    let marker: FileMarker = serde_json::from_value(
        section
            .get("marker")
            .cloned()
            .ok_or("inline file section is missing marker")?,
    )
    .map_err(|error| format!("inline file marker is invalid: {error}"))?;
    let data = section
        .get("data")
        .and_then(Value::as_str)
        .ok_or("inline file section is missing data")?
        .to_owned();
    let file = InlineFile { marker, data };
    file.bytes()?;
    Ok(Some(InlineWrite::Put(file)))
}
