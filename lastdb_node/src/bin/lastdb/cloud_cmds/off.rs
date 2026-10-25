//! A local credential pause does not cancel an explicit V2 backup.
use super::*;

pub(super) fn run(home: &Path, json: bool) -> Result<(), String> {
    let socket = home.join("data/folddb.sock");
    let value = if socket.exists() {
        let response = post_json(&socket, "/api/sync/cloud-off", &serde_json::json!({}))?;
        parse_json_response(&response, "cloud-off")?
    } else {
        let renamed = lastdb_node::cloud::pause_cloud_sync_file(home)?;
        serde_json::json!({
            "ok": true,
            "intent": "off",
            "file_state": lastdb_node::cloud::cloud_sync_file_state(home),
            "file_renamed": renamed,
            "explicit_backup_cancelled": false,
            "note": "Local credentials stay paused. Explicit V2 backups use backup-v2 close.",
        })
    };
    if !json {
        println!("Local cloud credentials are paused. Explicit V2 backups remain unchanged.");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|_| "cannot encode pause result")?
    );
    Ok(())
}
