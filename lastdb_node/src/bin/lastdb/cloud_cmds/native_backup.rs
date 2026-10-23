//! Native owner requests and explicit offline recovery commands.

use super::*;
use cloud_backup_protocol::Id;
use lastdb_node::native_cloud_backup::NativeBackupRequest;

#[path = "native_backup/offline.rs"]
mod offline;
#[path = "native_backup/setup.rs"]
mod setup;
pub(super) use setup::{is_setup, setup};

pub(super) fn run(home: &Path, action: NativeBackupCommand) -> Result<(), String> {
    let (request, json) = match action {
        NativeBackupCommand::Start { request_id, json } => {
            let id = Id::try_from(request_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()))
                .map_err(|_| "native backup request ID must be a canonical UUID".to_owned())?;
            // The caller can reuse this exact identity after an unknown submission.
            eprintln!("Native backup request ID: {}", id.as_str());
            (NativeBackupRequest::Start { request_id: id }, json)
        }
        NativeBackupCommand::Resume { json } => (NativeBackupRequest::Resume, json),
        NativeBackupCommand::Status { json } => (NativeBackupRequest::Status, json),
        NativeBackupCommand::Close { json } => (NativeBackupRequest::Close, json),
        action => return offline::run(home, action),
    };
    let socket = home.join("data/folddb.sock");
    let body = serde_json::to_value(request)
        .map_err(|_| "cannot encode native backup request".to_owned())?;
    let response =
        super::super::socket_client::post_json_admin(&socket, "/api/sync/native-backup", &body)?;
    let value = parse_json_response(&response, "native backup")?;
    print(&value, json)
}

pub(super) fn print(value: &serde_json::Value, json: bool) -> Result<(), String> {
    let text = serde_json::to_string_pretty(value)
        .map_err(|_| "cannot encode native backup result".to_owned())?;
    if !json {
        println!("Native backup:");
    }
    println!("{text}");
    Ok(())
}
