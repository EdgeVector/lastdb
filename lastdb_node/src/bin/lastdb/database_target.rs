use super::*;
use lastdb_uds::database_path::{self, DATABASES_PATH, DATABASE_SCOPE_PREFIX};
use std::borrow::Cow;

pub(crate) struct DatabaseTarget {
    pub(crate) socket: PathBuf,
    selector: Option<String>,
}

impl DatabaseTarget {
    pub(crate) fn open(data_dir: Option<PathBuf>, selector: Option<&str>) -> Result<Self, String> {
        let (_, socket) = resolve_client_home_and_socket(data_dir)?;
        let selector = selector.map(|value| resolve(&socket, value)).transpose()?;
        Ok(Self { socket, selector })
    }

    pub(crate) fn path<'p>(&self, path: &'p str) -> Cow<'p, str> {
        match self.selector.as_deref() {
            Some(selector) => Cow::Owned(format!(
                "{DATABASE_SCOPE_PREFIX}{selector}{}",
                path.strip_prefix("/api")
                    .expect("database path starts with /api")
            )),
            None => Cow::Borrowed(path),
        }
    }

    pub(crate) fn is_explicit(&self) -> bool {
        self.selector.is_some()
    }
}

fn resolve(socket: &Path, selector: &str) -> Result<String, String> {
    if !database_path::valid_selector(selector) {
        return Err("invalid database ID or name".into());
    }
    if selector == "personal" || cloud_backup_protocol::Id::try_from(selector).is_ok() {
        return Ok(selector.to_owned());
    }
    let response = read_databases(socket)?;
    let id = databases(&response)?
        .iter()
        .find(|database| database["name"].as_str() == Some(selector))
        .and_then(|database| database["id"].as_str())
        .filter(|id| cloud_backup_protocol::Id::try_from(*id).is_ok())
        .ok_or("database is not registered on this node")?;
    Ok(id.to_owned())
}

fn read_databases(socket: &Path) -> Result<serde_json::Value, String> {
    let raw = get_path(socket, DATABASES_PATH, Duration::from_secs(10))?;
    parse_json_response(&raw, "database list")
}

fn databases(response: &serde_json::Value) -> Result<&Vec<serde_json::Value>, String> {
    response["databases"]
        .as_array()
        .ok_or("missing database list".into())
}

pub(super) fn list_databases(data_dir: Option<PathBuf>, json: bool) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, None)?;
    let response = read_databases(&target.socket)?;
    if json {
        println!("{response}");
    } else {
        for database in databases(&response)? {
            let id = database["id"].as_str().ok_or("database has no ID")?;
            let name = database["name"].as_str().unwrap_or(id);
            let access = database["access"]
                .as_str()
                .ok_or("database has no access mode")?;
            let state = database["state"].as_str().ok_or("database has no state")?;
            println!("{name}\t{id}\t{access}\t{state}");
        }
    }
    Ok(())
}
