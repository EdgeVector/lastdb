//! Diff a desired selection against the share database and this query's ledger.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::PublishError;

/// One reconcile refuses a selection larger than this. One query page is the
/// same size, so a second page is a refusal rather than a scan.
pub const MAX_RECORDS: usize = 256;

/// Counts only. A receipt has no field values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub added: u32,
    pub updated: u32,
    pub removed: u32,
    pub unchanged: u32,
}

/// Primary key of one published record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordKey {
    pub namespace: String,
    pub schema: String,
    pub hash: String,
    pub range: Option<String>,
}

impl RecordKey {
    pub fn qualified_schema(&self) -> String {
        format!("{}/{}", self.namespace, self.schema)
    }
}

/// File marker compared by sha256. The name and media type are copied when
/// the sha differs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMark {
    pub sha256: String,
    pub name: String,
    pub media_type: String,
}

/// One destination row the reconciler has already read by key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveRow {
    pub key: RecordKey,
    pub revision: String,
    pub fields: BTreeMap<String, Value>,
    pub alternates: Value,
    pub blob: Option<FileMark>,
    pub inline: Option<FileMark>,
}

/// Shareable projection of one source row.
#[derive(Clone, Debug, PartialEq)]
pub struct Projected {
    pub key: RecordKey,
    pub fields: BTreeMap<String, Value>,
    pub blob: Option<FileMark>,
    pub inline: Option<FileMark>,
}

/// What the last successful reconcile published. Only these keys are removed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerEntry {
    pub namespace: String,
    pub schema: String,
    pub hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline: Option<String>,
}

impl LedgerEntry {
    pub fn key(&self) -> RecordKey {
        RecordKey {
            namespace: self.namespace.clone(),
            schema: self.schema.clone(),
            hash: self.hash.clone(),
            range: self.range.clone(),
        }
    }

    fn from_projected(row: &Projected) -> Self {
        Self {
            namespace: row.key.namespace.clone(),
            schema: row.key.schema.clone(),
            hash: row.key.hash.clone(),
            range: row.key.range.clone(),
            blob: row.blob.as_ref().map(|mark| mark.sha256.clone()),
            inline: row.inline.as_ref().map(|mark| mark.sha256.clone()),
        }
    }
}

/// One destination write. The file copies carry the field mutation when they
/// are present, so the caller does not also send a plain mutation.
#[derive(Clone, Debug, PartialEq)]
pub struct Write {
    pub key: RecordKey,
    pub fields: BTreeMap<String, Value>,
    pub creating: bool,
    pub fields_changed: bool,
    pub copy_blob: Option<FileMark>,
    pub copy_inline: Option<FileMark>,
    pub clear_blob: bool,
    pub clear_inline: bool,
    pub revision: Option<String>,
}

/// Remove one ledger key that is still present. Clear files before the delete.
#[derive(Clone, Debug, PartialEq)]
pub struct Remove {
    pub key: RecordKey,
    pub revision: String,
    pub fields: BTreeMap<String, Value>,
    pub alternates: Value,
    pub clear_blob: bool,
    pub clear_inline: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    Write(Write),
    Remove(Remove),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub steps: Vec<Step>,
    pub ledger: Vec<LedgerEntry>,
    pub receipt: Receipt,
    pub ledger_changed: bool,
}

impl LiveRow {
    pub fn from_query(namespace: &str, schema: &str, row: &Value) -> Result<Self, PublishError> {
        let key = row_key(namespace, schema, row)?;
        let revision = row
            .get("revision")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or(PublishError::new("query row has no revision"))?
            .to_owned();
        let fields = object_map(row.get("fields"));
        let alternates = row
            .get("alternates")
            .filter(|value| value.is_object())
            .cloned()
            .unwrap_or(Value::Object(Map::new()));
        Ok(Self {
            key,
            revision,
            fields,
            alternates,
            blob: marker(row, "file"),
            inline: marker(row, "inline_file"),
        })
    }
}

impl Projected {
    /// Keep the named fields and the file markers. Other fields stay behind.
    pub fn from_query(
        namespace: &str,
        schema: &str,
        shareable: &[String],
        row: &Value,
    ) -> Result<Self, PublishError> {
        let live = LiveRow::from_query(namespace, schema, row)?;
        let mut fields = BTreeMap::new();
        for name in shareable {
            if let Some(value) = live.fields.get(name) {
                if !value.is_null() {
                    fields.insert(name.clone(), value.clone());
                }
            }
        }
        Ok(Self {
            key: live.key,
            fields,
            blob: live.blob,
            inline: live.inline,
        })
    }
}

/// Last write wins for a repeated key. Ledger keys absent from `desired` are
/// removed only while they still exist. Keys in neither set are left alone.
pub fn plan(
    desired: Vec<Projected>,
    live: &BTreeMap<RecordKey, LiveRow>,
    ledger: &[LedgerEntry],
) -> Result<Plan, PublishError> {
    let desired = dedup(desired);
    if desired.len() > MAX_RECORDS {
        return Err(PublishError::new(
            "saved query selects more than 256 records",
        ));
    }
    let mut steps = Vec::new();
    let mut added = 0;
    let mut updated = 0;
    let mut unchanged = 0;
    let mut next_ledger = Vec::new();
    for (key, projected) in &desired {
        next_ledger.push(LedgerEntry::from_projected(projected));
        match live.get(key) {
            None => {
                added += 1;
                steps.push(Step::Write(write_for(projected, None)));
            }
            Some(row) if same(projected, row) => unchanged += 1,
            Some(row) => {
                updated += 1;
                steps.push(Step::Write(write_for(projected, Some(row))));
            }
        }
    }
    let removed = push_removals(&mut steps, &desired, live, ledger);
    let ledger_changed = ledger_differs(&next_ledger, ledger);
    Ok(Plan {
        steps,
        ledger: next_ledger,
        receipt: Receipt {
            added,
            updated,
            removed,
            unchanged,
        },
        ledger_changed,
    })
}

fn push_removals(
    steps: &mut Vec<Step>,
    desired: &BTreeMap<RecordKey, Projected>,
    live: &BTreeMap<RecordKey, LiveRow>,
    ledger: &[LedgerEntry],
) -> u32 {
    let mut removed = 0;
    for entry in ledger {
        let key = entry.key();
        if desired.contains_key(&key) {
            continue;
        }
        let Some(row) = live.get(&key) else {
            continue;
        };
        removed += 1;
        steps.push(Step::Remove(Remove {
            key,
            revision: row.revision.clone(),
            fields: row.fields.clone(),
            alternates: row.alternates.clone(),
            clear_blob: row.blob.is_some(),
            clear_inline: row.inline.is_some(),
        }));
    }
    removed
}

fn dedup(desired: Vec<Projected>) -> BTreeMap<RecordKey, Projected> {
    let mut out = BTreeMap::new();
    for row in desired {
        out.insert(row.key.clone(), row);
    }
    out
}

fn same(projected: &Projected, row: &LiveRow) -> bool {
    projected.fields == row.fields
        && sha(&projected.blob) == sha(&row.blob)
        && sha(&projected.inline) == sha(&row.inline)
}

fn sha(mark: &Option<FileMark>) -> Option<&str> {
    mark.as_ref().map(|mark| mark.sha256.as_str())
}

fn write_for(projected: &Projected, row: Option<&LiveRow>) -> Write {
    Write {
        key: projected.key.clone(),
        fields: projected.fields.clone(),
        creating: row.is_none(),
        fields_changed: row.is_none_or(|row| row.fields != projected.fields),
        copy_blob: changed_mark(projected.blob.clone(), row.and_then(|row| row.blob.clone())),
        copy_inline: changed_mark(
            projected.inline.clone(),
            row.and_then(|row| row.inline.clone()),
        ),
        clear_blob: row.is_some_and(|row| row.blob.is_some()) && projected.blob.is_none(),
        clear_inline: row.is_some_and(|row| row.inline.is_some()) && projected.inline.is_none(),
        revision: row.map(|row| row.revision.clone()),
    }
}

fn changed_mark(desired: Option<FileMark>, live: Option<FileMark>) -> Option<FileMark> {
    let differs = desired
        .as_ref()
        .is_some_and(|mark| sha(&live) != Some(mark.sha256.as_str()));
    if differs {
        desired
    } else {
        None
    }
}

fn ledger_differs(next: &[LedgerEntry], previous: &[LedgerEntry]) -> bool {
    let mut left: Vec<_> = next.iter().map(LedgerEntry::key_mark).collect();
    let mut right: Vec<_> = previous.iter().map(LedgerEntry::key_mark).collect();
    left.sort();
    right.sort();
    left != right
}

impl LedgerEntry {
    fn key_mark(&self) -> (RecordKey, Option<String>, Option<String>) {
        (self.key(), self.blob.clone(), self.inline.clone())
    }
}

fn row_key(namespace: &str, schema: &str, row: &Value) -> Result<RecordKey, PublishError> {
    let key = row
        .get("key")
        .ok_or(PublishError::new("query row has no key"))?;
    let hash = key
        .get("hash")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(PublishError::new("query row has no key"))?
        .to_owned();
    let range = key
        .get("range")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    Ok(RecordKey {
        namespace: namespace.to_owned(),
        schema: schema.to_owned(),
        hash,
        range,
    })
}

fn object_map(value: Option<&Value>) -> BTreeMap<String, Value> {
    let mut fields = BTreeMap::new();
    let Some(Value::Object(map)) = value else {
        return fields;
    };
    for (key, value) in map {
        fields.insert(key.clone(), value.clone());
    }
    fields
}

fn marker(row: &Value, name: &str) -> Option<FileMark> {
    let value = row.pointer(&format!("/metadata/{name}"))?;
    let sha256 = value.get("sha256")?.as_str()?.to_owned();
    if sha256.is_empty() {
        return None;
    }
    let name = value
        .get("name")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or("file")
        .to_owned();
    let media_type = value
        .get("media_type")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or("application/octet-stream")
        .to_owned();
    Some(FileMark {
        sha256,
        name,
        media_type,
    })
}
