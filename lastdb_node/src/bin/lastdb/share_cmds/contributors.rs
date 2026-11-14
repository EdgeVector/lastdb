use super::named;
use crate::ShareContributorCommand;
use std::path::PathBuf;

pub(super) fn run(
    data_dir: Option<PathBuf>,
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
            data_dir,
            db,
            recipient: &recipient,
            recipient_key,
            out: &out,
            regrant,
            expires_in_hours: expires_in_hours.unwrap_or(24),
        }),
        ShareContributorCommand::Revoke { recipient_key } => {
            named::revoke(data_dir, db, recipient_key)
        }
    }
}
