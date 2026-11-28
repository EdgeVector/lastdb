//! The point reads of one `/v1/read` request, and the reply.
//!
//! The point reads of one schema share one schema lookup and one storage read.
//! Their records are written into the reply from the stored records, without a
//! copy into JSON values first. The reply bytes are the bytes the per-read path
//! writes: `serde_json` objects list their keys in sorted order, and each map
//! here lists its keys in that order.

use fold_db::fold_db_core::query::PointRecord;
use fold_db::record_value::{FileMarker, StoredRecord};
use fold_db::schema::types::KeyValue;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::{json, Value};

use super::{database, key_filter, require_exact_name, require_read, ReadItem};
use crate::exec::reply::content_free;
use crate::exec::schema_routes::resolve_schema_name;
use crate::host::Host;
use crate::lastdb_uds::uds_http::UdsResponse;

/// What a reply reserves for each record before it writes. A record averages
/// about 14 KiB; reserving once avoids a copy each time the buffer doubles.
const RECORD_RESERVE: usize = 16 * 1024;
const OTHER_RESERVE: usize = 1024;

/// One result of `/v1/read`.
pub(super) enum Answer {
    /// A result the per-read path built.
    Built(Value),
    /// A point read, answered from its stored record.
    Point(PointRecord),
}

/// The point reads among `items`, answered in one read per schema and database.
/// `None` where an item is not a point read, or where this path cannot answer it;
/// the per-read path then reads that item and gives any error.
pub(super) async fn read(host: &Host, items: &[ReadItem]) -> Vec<Option<PointRecord>> {
    let mut found = vec![None; items.len()];
    for (database, schema, group) in groups(items) {
        let Some(answers) = read_group(host, database, schema, items, &group).await else {
            continue;
        };
        for (at, answer) in group.into_iter().zip(answers) {
            // A record with a file goes through `file_place`, on the per-read path.
            found[at] = answer
                .filter(|(_, record)| record.as_ref().is_none_or(|record| record.file.is_none()));
        }
    }
    found
}

/// A plain read of one key: no paging, no alternate key, no file request.
fn is_point(item: &ReadItem) -> bool {
    let keyed = item
        .key
        .as_ref()
        .and_then(|key| key.hash.as_deref())
        .is_some_and(|hash| !hash.is_empty());
    keyed
        && item.range.is_none()
        && item.via.is_none()
        && item.limit.is_none()
        && item.cursor.is_none()
        && !item.keys_only
        && !item.file
        && item.file_dir.is_none()
        && require_read(&item.schema).is_ok()
}

/// The point reads grouped by database and schema, in request order.
fn groups(items: &[ReadItem]) -> Vec<(Option<&str>, &str, Vec<usize>)> {
    let mut groups: Vec<(Option<&str>, &str, Vec<usize>)> = Vec::new();
    for (at, item) in items.iter().enumerate().filter(|(_, item)| is_point(item)) {
        let place = (item.database.as_deref(), item.schema.as_str());
        match groups.iter_mut().find(|(d, s, _)| (*d, *s) == place) {
            Some((_, _, group)) => group.push(at),
            None => groups.push((place.0, place.1, vec![at])),
        }
    }
    groups
}

async fn read_group(
    host: &Host,
    database: Option<&str>,
    schema: &str,
    items: &[ReadItem],
    group: &[usize],
) -> Option<Vec<Option<PointRecord>>> {
    let filters = group
        .iter()
        .map(|&at| items[at].key.as_ref().and_then(|key| key_filter(key).ok()))
        .collect::<Option<Vec<_>>>()?;
    let answers = database::read_in(database, host, async {
        require_exact_name(host, schema)?;
        let Some(canonical) = resolve_schema_name(host, schema)? else {
            return Ok(None);
        };
        let records = host.records();
        let points = records
            .query_executor()
            .point_records(&canonical, filters)
            .await?;
        Ok(Some(points))
    })
    .await;
    answers.ok().flatten()
}

/// `{"results": [...]}`, as `json_ok` writes it.
pub(super) fn reply(answers: &[Answer]) -> UdsResponse {
    let reserve = answers
        .iter()
        .map(|answer| match answer {
            Answer::Point((_, Some(_))) => RECORD_RESERVE,
            Answer::Point((_, None)) | Answer::Built(_) => OTHER_RESERVE,
        })
        .sum::<usize>();
    let mut body = Vec::with_capacity(reserve + OTHER_RESERVE);
    match serde_json::to_writer(&mut body, &Reply(answers)) {
        Ok(()) => UdsResponse::new(200, "OK", body).with_header("Content-Type", "application/json"),
        Err(_) => content_free(500, "Internal Server Error"),
    }
}

struct Reply<'a>(&'a [Answer]);

impl Serialize for Reply<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("results", self.0)?;
        map.end()
    }
}

impl Serialize for Answer {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Built(value) => value.serialize(serializer),
            Self::Point((key, record)) => {
                // A point read has no next page: the per-read path writes null.
                let records = record.as_deref().map(|record| Row { key, record });
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("next_cursor", &Value::Null)?;
                map.serialize_entry("records", &records.as_slice())?;
                map.end()
            }
        }
    }
}

/// One record of a point read, in the shape of `shape` in the per-read path.
struct Row<'a> {
    key: &'a KeyValue,
    record: &'a StoredRecord,
}

impl Serialize for Row<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let r = self.record;
        let authors = usize::from(r.created_by.is_some()) + usize::from(r.updated_by.is_some());
        let mut map = serializer.serialize_map(Some(5 + authors))?;
        // An author key is omitted when the record has none, never null.
        if let Some(created_by) = &r.created_by {
            map.serialize_entry("created_by", created_by)?;
        }
        // `read` answers only a record without a file. `file_place` handles the others.
        map.serialize_entry("file", &Value::Null)?;
        map.serialize_entry("inline_file", &inline_reply(r.inline_file.as_ref()))?;
        map.serialize_entry("key", self.key)?;
        map.serialize_entry("record", &r.fields)?;
        map.serialize_entry("revision", &r.stamp.revision())?;
        if let Some(updated_by) = &r.updated_by {
            map.serialize_entry("updated_by", updated_by)?;
        }
        map.end()
    }
}

fn inline_reply(marker: Option<&FileMarker>) -> Value {
    match marker {
        None => Value::Null,
        Some(marker) => json!({
            "name": marker.name,
            "media_type": marker.media_type,
            "size": marker.size_bytes,
        }),
    }
}
