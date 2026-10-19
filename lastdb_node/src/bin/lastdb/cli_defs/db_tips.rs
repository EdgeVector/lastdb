//! Clap subcommands for `lastdb db`: tip-history retention, drain and migration verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbTipsCommand {
    /// Rewrite legacy fat `mk:` tip values to thin (atom_uuid + written_at + device_id).
    ///
    /// Same keys, thinner payloads. Default is dry-run. Pass `--execute` to rewrite.
    /// Safe under dual-read: readers accept both formats; local writes already emit thin.
    MigrateThinTips {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Move `photos/Photo.file_bytes` (inline base64) into local CAS blobs.
    ///
    /// Default is dry-run. Pass `--execute` to write CAS + clear the field.
    /// Follow with `gc-atoms --execute` to drop tombstoned tip history + orphan atoms.
    MigratePhotoBlobs {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
}
