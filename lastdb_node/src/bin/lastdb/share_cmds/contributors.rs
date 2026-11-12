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
            recipient,
            recipient_key,
            out,
            regrant,
            expires_in_hours,
        } => named::grant(named::InviteOptions {
            identity_home,
            db,
            recipient: &recipient,
            recipient_key,
            out: &out,
            regrant,
            expires_in_hours: expires_in_hours.unwrap_or(24),
        }),
        ShareContributorCommand::Revoke { recipient_key } => {
            named::revoke(identity_home, db, recipient_key)
        }
        ShareContributorCommand::Accept { .. } => {
            unreachable!("grant attachment is routed to the node")
        }
    }
}
