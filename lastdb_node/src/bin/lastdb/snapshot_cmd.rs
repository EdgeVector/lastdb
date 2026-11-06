//! One owner invocation for publication and its queued writer changes.

use super::{parse_json_response, socket_client, SnapshotArgs};
use cloud_backup_protocol::Id;
use lastdb_node::exec::SharedSnapshotRequest;
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
    let snapshot = response["snapshot_id"]
        .as_str()
        .ok_or("missing published snapshot ID")?;
    let position = response["writer_uploaded_through"]
        .as_u64()
        .ok_or("missing uploaded writer position")?;
    let r2_files = response["r2_files"]
        .as_u64()
        .ok_or("missing R2 file count")?;
    let b2_files = response["b2_files"]
        .as_u64()
        .ok_or("missing B2 file count")?;
    if args.json {
        println!(
            "{}",
            serde_json::json!({
                "snapshot_id": snapshot,
                "writer_uploaded_through": position,
                "r2_files": r2_files,
                "b2_files": b2_files,
            })
        );
    } else {
        println!("Published shared snapshot {snapshot}");
        println!("Uploaded {r2_files} disk files to R2 and {b2_files} to B2");
        println!("Uploaded shared writer through {position}");
    }
    Ok(())
}
