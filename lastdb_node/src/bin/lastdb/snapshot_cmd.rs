//! One owner invocation for publication and its queued writer changes.

use super::{parse_json_response, socket_client, SnapshotArgs};
use cloud_backup_protocol::Id;
use lastdb_node::shared_snapshot::SharedSnapshotRequest;
use std::path::Path;

pub(super) fn run(home: Option<&Path>, args: SnapshotArgs) -> Result<(), String> {
    let home = home
        .filter(|path| path.is_absolute())
        .ok_or("snapshot requires an explicit absolute --data-dir")?;
    lastdb_node::offline_home::refuse_primary(home)?;
    let request = SharedSnapshotRequest {
        device_id: Id::try_from(args.device_id)
            .map_err(|_| "snapshot device ID must be a canonical UUID")?,
        device_key_file: args.device_key_file,
        workspace: args.workspace,
    };
    let body = serde_json::to_value(request).map_err(|_| "cannot encode snapshot request")?;
    let response = socket_client::post_json_admin(
        &home.join("data/folddb.sock"),
        "/api/sync/shared-snapshot",
        &body,
    )?;
    let response = parse_json_response(&response, "shared snapshot")?;
    let value = response
        .get("data")
        .ok_or("missing shared snapshot result")?;
    if args.json {
        println!("{value}");
    } else {
        let snapshot = value["snapshot_id"]
            .as_str()
            .ok_or("missing published snapshot ID")?;
        let position = value["writer_uploaded_through"]
            .as_u64()
            .ok_or("missing uploaded writer position")?;
        println!("Published shared snapshot {snapshot}");
        println!("Uploaded shared writer through {position}");
    }
    Ok(())
}
