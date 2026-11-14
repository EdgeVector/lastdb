//! Database target resolution and `lastdb db` catalog commands (`create`, `list`, `select`).

use super::*;
use lastdb_node::lastdb_uds::{valid_selector, DATABASES_PATH, DATABASE_SCOPE_PREFIX};
use serde_json::Value;
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(crate) struct DatabaseTarget {
    pub(crate) socket: PathBuf,
    pub(super) selector: Option<String>,
}

impl DatabaseTarget {
    pub(crate) fn open(data_dir: Option<PathBuf>, selector: Option<&str>) -> Result<Self, String> {
        let (_, socket) = resolve_client_home_and_socket(data_dir)?;
        let selector = selector.map(|v| resolve_database(&socket, v)).transpose()?;
        Ok(Self { socket, selector })
    }

    pub(crate) fn path<'p>(&self, path: &'p str) -> Cow<'p, str> {
        let Some(sel) = self.selector.as_deref() else {
            return Cow::Borrowed(path);
        };
        let suffix = path
            .strip_prefix("/api")
            .expect("database path starts with /api");
        Cow::Owned(format!("{DATABASE_SCOPE_PREFIX}{sel}{suffix}"))
    }

    pub(crate) fn selector(&self) -> Option<&str> {
        self.selector.as_deref()
    }
}

fn fetch_databases(socket: &Path) -> Result<Value, String> {
    let raw = get_path(socket, DATABASES_PATH, Duration::from_secs(10))?;
    parse_json_response(&raw, "database list")
}

fn resolve_database(socket: &Path, selector: &str) -> Result<String, String> {
    if !valid_selector(selector) {
        return Err("invalid database ID or name".into());
    }
    if selector == "personal" || cloud_backup_protocol::Id::try_from(selector.to_owned()).is_ok() {
        return Ok(selector.to_owned());
    }
    fetch_databases(socket)?["databases"]
        .as_array()
        .ok_or("missing database list")?
        .iter()
        .find_map(|db| {
            (db["name"].as_str() == Some(selector)).then_some(())?;
            let id = db["id"].as_str()?;
            cloud_backup_protocol::Id::try_from(id.to_owned()).ok()?;
            Some(id.to_owned())
        })
        .ok_or_else(|| "database is not registered on this node".into())
}

pub(super) fn list_databases(data_dir: Option<PathBuf>, json_only: bool) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, None)?;
    let resp = fetch_databases(&target.socket)?;
    if json_only {
        println!("{resp}");
        return Ok(());
    }
    for db in resp["databases"]
        .as_array()
        .ok_or("missing database list")?
    {
        print_database_row(db)?;
    }
    Ok(())
}

pub(super) fn create_database(
    data_dir: Option<PathBuf>,
    name: &str,
    private: bool,
    spec_name: Option<&str>,
    spec_version: Option<u32>,
    json: bool,
) -> Result<(), String> {
    let spec = create_spec(name, spec_name, spec_version)?;
    let target = DatabaseTarget::open(data_dir, None)?;
    let mut body = serde_json::json!({ "name": name });
    if private {
        body["kind"] = Value::String("personal".into());
    }
    if let Some(spec) = spec {
        body["spec"] = spec;
    }
    let raw = post_json_with_timeout(
        &target.socket,
        DATABASES_PATH,
        &body,
        Duration::from_secs(30),
    )?;
    let response = parse_json_response(&raw, "database create")?;
    let database = response
        .get("database")
        .ok_or("database create returned no database")?;
    print_database(database, json)
}

pub(super) fn select_database(
    data_dir: Option<PathBuf>,
    selector: &str,
    json: bool,
) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, None)?;
    print_database(&registered_database(&target.socket, selector)?, json)
}

pub(super) fn registered_database(socket: &Path, selector: &str) -> Result<Value, String> {
    if !valid_selector(selector) {
        return Err("invalid database selector".into());
    }
    let mut body = fetch_databases(socket)?;
    let list = body["databases"]
        .as_array_mut()
        .ok_or("missing database list")?;
    let index = list
        .iter()
        .position(|db| db["id"].as_str() == Some(selector))
        .or_else(|| {
            list.iter()
                .position(|db| db["name"].as_str() == Some(selector))
        })
        .ok_or("database is not registered")?;
    Ok(list.swap_remove(index))
}

fn create_spec(
    name: &str,
    spec_name: Option<&str>,
    spec_version: Option<u32>,
) -> Result<Option<Value>, String> {
    if !valid_database_name(name) {
        return Err("invalid database name".into());
    }
    let spec = match (spec_name, spec_version) {
        (None, None) => None,
        (Some(spec_name), Some(version)) if valid_selector(spec_name) && version >= 1 => {
            Some(serde_json::json!({ "name": spec_name, "version": version }))
        }
        _ => return Err("invalid spec binding".into()),
    };
    Ok(spec)
}

/// Same create-name rule as the owned-database host: a label, not `personal`, not a UUID.
fn valid_database_name(name: &str) -> bool {
    valid_selector(name)
        && !name.eq_ignore_ascii_case("personal")
        && uuid::Uuid::parse_str(name).is_err()
}

fn print_database(database: &Value, json: bool) -> Result<(), String> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(database).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    print_database_row(database)
}

fn print_database_row(database: &Value) -> Result<(), String> {
    let id = database["id"].as_str().ok_or("database has no ID")?;
    let name = database["name"].as_str().unwrap_or(id);
    let access = database["access"]
        .as_str()
        .ok_or("database has no access mode")?;
    let state = database["state"].as_str().ok_or("database has no state")?;
    println!("{name}\t{id}\t{access}\t{state}");
    Ok(())
}
