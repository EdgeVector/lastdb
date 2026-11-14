use super::{id, SCHEMAS};
use crate::{get_query_body, parse_json_response, post_json_admin, share_file, DatabaseTarget};
use fold_db::schema::types::KeyValue;
use serde_json::{json, Value};
use std::path::Path;

pub(super) const QUERY_BATCH_PATH: &str = "/api/queries/batch";

pub(super) fn list(target: &DatabaseTarget) -> Result<(), String> {
    let database = share_id(target)?;
    let files = share_file::list_files_by_key(&SCHEMAS, |schema, keys, field| {
        read_field(target, schema, keys, field)
    })?;
    println!("{}", json!({ "database": database, "files": files }));
    Ok(())
}

pub(super) fn save(target: &DatabaseTarget, file: String, out: &Path) -> Result<(), String> {
    let database = share_id(target)?;
    share_file::get_file_by_key(&SCHEMAS, &id(file)?, out, |schema, keys, field| {
        read_field(target, schema, keys, field)
    })?;
    println!("{}", json!({ "database": database, "path": out }));
    Ok(())
}

fn share_id(target: &DatabaseTarget) -> Result<&str, String> {
    target
        .selector()
        .filter(|selector| *selector != "personal")
        .ok_or_else(|| "share files requires --db with a share ID or name".into())
}

fn read_field(
    target: &DatabaseTarget,
    schema: &str,
    keys: &[KeyValue],
    field: &str,
) -> Result<Vec<Option<String>>, String> {
    query(&target.socket, &target.path(QUERY_BATCH_PATH), schema, keys)?
        .into_iter()
        .map(|record| {
            record
                .map(|mut row| {
                    let data = row
                        .get_mut("fields")
                        .and_then(|fields| fields.get_mut(field))
                        .map(Value::take);
                    match data {
                        Some(Value::String(data)) => Ok(data),
                        _ => Err("file share record is missing its data field".into()),
                    }
                })
                .transpose()
        })
        .collect()
}

pub(super) fn query(
    socket: &Path,
    path: &str,
    schema: &str,
    keys: &[KeyValue],
) -> Result<Vec<Option<Value>>, String> {
    let queries = keys
        .iter()
        .map(|key| {
            let hash = key
                .hash
                .as_deref()
                .ok_or("file sharing requires a hash key")?;
            Ok(get_query_body(schema, hash, key.range.as_deref()))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let response = post_json_admin(socket, path, &json!({"queries":queries}))?;
    let value = parse_json_response(&response, path)?;
    let replies = value["results"]
        .as_array()
        .filter(|v| v.len() == keys.len())
        .ok_or("file-sharing read returned incomplete results")?;
    replies
        .iter()
        .zip(keys)
        .map(|(reply, key)| exact_row(reply, key))
        .collect()
}

fn exact_row(reply: &Value, key: &KeyValue) -> Result<Option<Value>, String> {
    let response = &reply["response"];
    if reply["status"].as_u64() != Some(200)
        || response["ok"].as_bool() != Some(true)
        || response["has_more"].as_bool() != Some(false)
    {
        return Err("file-sharing read did not return a complete result".into());
    }
    let rows = response["results"]
        .as_array()
        .ok_or("file-sharing read omitted its rows")?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] if row["key"] == json!(key) => Ok(Some(row.clone())),
        _ => Err("file-sharing read returned a different key".into()),
    }
}
