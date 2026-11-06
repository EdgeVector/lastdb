//! Record-level data command dispatch for the `lastdb` CLI.

use super::*;

/// Record-level data commands: mutate, get-keys, get.
pub(super) fn record_command(data_dir: Option<PathBuf>, command: Command) -> Result<(), String> {
    match command {
        Command::Mutate {
            request_file,
            durable,
        } => mutate_command(data_dir, &request_file, durable),
        Command::GetKeys(args) => list_record_keys(data_dir, &args),
        Command::Get {
            schema,
            key_hash,
            key_range,
            json,
            hydrate,
        } => get_record(
            data_dir,
            &schema,
            key_hash.as_deref(),
            key_range.as_deref(),
            json,
            hydrate.as_deref(),
        ),
        _ => unreachable!("run() routes only record commands here"),
    }
}
