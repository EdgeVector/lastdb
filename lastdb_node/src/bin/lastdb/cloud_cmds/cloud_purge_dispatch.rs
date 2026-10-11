use super::*;
use fold_db::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};

pub(super) fn run(home: &Path, action: CloudPurgeResyncCommand) -> Result<(), String> {
    match action {
        CloudPurgeResyncCommand::Begin {
            org,
            request_id,
            json,
        } => {
            let account = org.map(|id| CloudAccount {
                kind: CloudAccountKind::Org,
                id,
            });
            cloud_purge::cloud_purge_resync(
                home,
                "begin",
                request_id.as_deref(),
                account.as_ref(),
                None,
                json,
            )
        }
        CloudPurgeResyncCommand::Status {
            job,
            proof_receipt,
            json,
        } => cloud_purge::cloud_purge_resync(
            home,
            "status",
            job.as_deref(),
            None,
            proof_receipt.as_deref(),
            json,
        ),
        CloudPurgeResyncCommand::Resume { job, json } => {
            cloud_purge::cloud_purge_resync(home, "resume", Some(&job), None, None, json)
        }
        CloudPurgeResyncCommand::Cancel { job, json } => {
            cloud_purge::cloud_purge_resync(home, "cancel", Some(&job), None, None, json)
        }
    }
}
