use super::named;
use crate::ShareContributorCommand;
use std::path::Path;

pub(super) fn run(
    identity_home: &Path,
    db: Option<&str>,
    action: ShareContributorCommand,
) -> Result<(), String> {
    match action {
        ShareContributorCommand::Grant {
            home,
            recipient,
            recipient_key,
            rules,
            operation_id,
            expected_version,
            out,
            regrant,
            expires_in_hours,
        } => named::grant(
            identity_home,
            db,
            home.as_deref(),
            &recipient,
            recipient_key,
            rules.as_deref(),
            operation_id,
            expected_version,
            &out,
            regrant,
            expires_in_hours,
        ),
        ShareContributorCommand::Revoke {
            home,
            recipient_key,
            expected_version,
        } => named::revoke(
            identity_home,
            db,
            home.as_deref(),
            recipient_key,
            expected_version,
        ),
        ShareContributorCommand::Accept { .. } => {
            unreachable!("grant attachment is routed to the node")
        }
    }
}
