//! Record-level data command dispatch for the `lastdb` CLI.

use super::*;

/// Record-level data commands: mutate, get-keys, get.
pub(super) fn record_command(target: DatabaseTarget, command: Command) -> Result<(), String> {
    match command {
        Command::Mutate {
            request_file,
            durable,
            mutation_id,
        } => mutate_command(&target, &request_file, durable, mutation_id),
        Command::GetKeys(args) => list_record_keys(&target, &args),
        Command::Get {
            schema,
            key_hash,
            key_range,
            json,
            hydrate,
            hydrate_inline,
        } => get_record(
            &target,
            &schema,
            key_hash.as_deref(),
            key_range.as_deref(),
            json,
            hydrate.as_deref(),
            hydrate_inline.as_deref(),
        ),
        _ => unreachable!("run() routes only record commands here"),
    }
}
