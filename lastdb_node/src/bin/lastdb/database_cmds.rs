//! Owned-database create and select. The host registry stays with the node.

use super::*;
use lastdb_node::lastdb_uds::database_path::{self, DATABASES_PATH};

const RESERVED_SPEC_NAMES: &[&str] = &["social", "medical", "finance", "work"];

pub(super) fn create_database(
    data_dir: Option<PathBuf>,
    name: &str,
    spec_name: Option<&str>,
    spec_version: Option<u32>,
    json: bool,
) -> Result<(), String> {
    let spec = create_spec(name, spec_name, spec_version)?;
    let target = DatabaseTarget::open(data_dir, None)?;
    let mut body = serde_json::json!({ "name": name });
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
    if !database_path::valid_selector(selector) {
        return Err("invalid database selector".into());
    }
    let target = DatabaseTarget::open(data_dir, None)?;
    let raw = get_path(&target.socket, DATABASES_PATH, Duration::from_secs(10))?;
    let response = parse_json_response(&raw, "database list")?;
    let list = response["databases"]
        .as_array()
        .ok_or("missing database list")?;
    let database = list
        .iter()
        .find(|db| db["id"].as_str() == Some(selector))
        .or_else(|| list.iter().find(|db| db["name"].as_str() == Some(selector)))
        .ok_or("database is not registered")?;
    print_database(database, json)
}

fn create_spec(
    name: &str,
    spec_name: Option<&str>,
    spec_version: Option<u32>,
) -> Result<Option<serde_json::Value>, String> {
    if !valid_database_name(name) {
        return Err("invalid database name".into());
    }
    if name == "graph" {
        return Err("database name is reserved and not provisioned".into());
    }
    let spec = match (spec_name, spec_version) {
        (None, None) => None,
        (Some(spec_name), Some(version))
            if database_path::valid_selector(spec_name) && version >= 1 =>
        {
            Some(serde_json::json!({ "name": spec_name, "version": version }))
        }
        _ => return Err("invalid spec binding".into()),
    };
    if RESERVED_SPEC_NAMES.contains(&name)
        && spec.as_ref().and_then(|s| s["name"].as_str()) != Some(name)
    {
        return Err("reserved database name requires its spec".into());
    }
    Ok(spec)
}

/// Same create-name rule as the owned-database host: a label, not `personal`, not a UUID.
fn valid_database_name(name: &str) -> bool {
    database_path::valid_selector(name)
        && !name.eq_ignore_ascii_case("personal")
        && uuid::Uuid::parse_str(name).is_err()
}

fn print_database(database: &serde_json::Value, json: bool) -> Result<(), String> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(database).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    let id = database["id"].as_str().ok_or("database has no ID")?;
    let name = database["name"].as_str().unwrap_or(id);
    let access = database["access"]
        .as_str()
        .ok_or("database has no access mode")?;
    let state = database["state"].as_str().ok_or("database has no state")?;
    println!("{name}\t{id}\t{access}\t{state}");
    Ok(())
}
