//! Explicit V2 owner requests and offline recovery. No automatic activation.
use super::*;
use cloud_backup_protocol::Id;
use lastdb_node::byte_cloud_backup::ByteBackupRequest;

#[path = "byte_backup/offline.rs"]
mod offline;

pub(super) fn run(home: &Path, action: ByteBackupCommand) -> Result<(), String> {
    let (request, json) = match action {
        ByteBackupCommand::Start { request_id, json } => {
            let request_id =
                Id::try_from(request_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()))
                    .map_err(|_| "V2 request ID must be a canonical UUID")?;
            eprintln!("V2 backup request ID: {}", request_id.as_str());
            (ByteBackupRequest::Start { request_id }, json)
        }
        ByteBackupCommand::Resume { json } => (ByteBackupRequest::Resume, json),
        ByteBackupCommand::Status { json } => (ByteBackupRequest::Status, json),
        ByteBackupCommand::Close { json } => (ByteBackupRequest::Close, json),
        action => return offline::run(home, action),
    };
    let body = serde_json::to_value(request).map_err(|_| "cannot encode V2 owner request")?;
    let response = super::super::socket_client::post_json_admin(
        &home.join("data/folddb.sock"),
        "/api/sync/byte-backup-v2",
        &body,
    )?;
    print(&parse_json_response(&response, "V2 backup")?, json)
}

pub(super) fn print(value: &serde_json::Value, json: bool) -> Result<(), String> {
    if !json {
        println!("V2 backup:");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|_| "cannot encode V2 result")?
    );
    Ok(())
}
