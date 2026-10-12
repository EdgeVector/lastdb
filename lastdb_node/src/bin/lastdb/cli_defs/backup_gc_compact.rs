use super::*;

#[derive(Args, Debug)]
pub(crate) struct BackupGcCompactArgs {
    /// Exact terminal backup job UUID.
    #[arg(long)]
    pub(crate) job_id: String,

    /// Remove old sources only after exact packed byte equality checks.
    /// Loose intent, unknown, and uncertain_write receipts stay in place.
    #[arg(long)]
    pub(crate) remove_sources: bool,

    /// Emit the local receipt report as JSON without cloud object keys.
    #[arg(long)]
    pub(crate) json: bool,
}
