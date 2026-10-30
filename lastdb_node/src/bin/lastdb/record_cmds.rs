//! Record-level data command dispatch for the `lastdb` CLI.

use super::*;

/// Record-level data commands: mutate, list, get-keys, get.
pub(super) fn record_command(data_dir: Option<PathBuf>, command: Command) -> Result<(), String> {
    match command {
        Command::Mutate {
            schema,
            mutation_type,
            key_hash,
            key_range,
            key_range_prefix,
            must_exist,
            durable,
            json: _,
        } => mutate_command(
            data_dir,
            &schema,
            &mutation_type,
            key_hash.as_deref(),
            key_range.as_deref(),
            key_range_prefix.as_deref(),
            MutateRequestOptions {
                must_exist,
                durable,
            },
        ),
        Command::List(args) => list_record_keys(data_dir, &args, "list"),
        Command::GetKeys(args) => list_record_keys(data_dir, &args, "get-keys"),
        Command::Get {
            schema,
            key_hash,
            key_range,
            json,
        } => get_record(
            data_dir,
            &schema,
            key_hash.as_deref(),
            key_range.as_deref(),
            json,
        ),
        _ => unreachable!("run() routes only record commands here"),
    }
}
