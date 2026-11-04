//! One definition of the history key builders, shared with readers.

use super::{digest, invalid};
use crate::schema::SchemaError;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::RecordStamp;

pub(crate) fn record_prefix(record: &str, field: Option<&str>) -> Result<String, SchemaError> {
    let space = record
        .strip_prefix(RECORD_PREFIX)
        .and_then(|rest| rest.split(':').next())
        .filter(|space| !space.is_empty())
        .ok_or_else(|| invalid("the location is not a record key"))?;
    let mut base = format!(
        "{}:{space}:{}:",
        super::super::HISTORY_RECORD_FAMILY,
        digest(record.as_bytes())
    );
    if let Some(field) = field {
        base.push_str(field);
        base.push(':');
    }
    Ok(base)
}

pub(super) fn entry_key(
    record: &str,
    field: &str,
    stamp: &RecordStamp,
) -> Result<String, SchemaError> {
    require_record_key(record)?;
    let identity = serde_json::to_vec(&(record, field, stamp)).map_err(invalid)?;
    Ok(format!(
        "{}{}",
        record_prefix(record, Some(field))?,
        digest(&identity)
    ))
}

pub(crate) fn count_key(record: &str) -> String {
    format!(
        "{}:{}",
        super::super::HISTORY_COUNT_FAMILY,
        digest(record.as_bytes())
    )
}

pub(super) fn write_count_key(record: &str) -> Result<String, SchemaError> {
    require_record_key(record)?;
    Ok(count_key(record))
}

pub(crate) fn policy_key(field: &str) -> String {
    format!("{}:{field}", super::super::HISTORY_POLICY_FAMILY)
}

fn require_record_key(record: &str) -> Result<(), SchemaError> {
    if !record.starts_with(RECORD_PREFIX) {
        return Err(invalid("the location is not a record key"));
    }
    Ok(crate::storage::current_keys::validate(
        super::super::MAIN_NAMESPACE,
        record.as_bytes(),
    )?)
}
