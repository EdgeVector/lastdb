//! `lastdb-maintenance reap`: drop the keys that a plan names from a stopped
//! store.
//!
//! Exit codes: 0 ok; 2 usage or guard refusal; 4 gate mismatch; 5 internal
//! error. A run without `--execute` changes no byte of the store. It exits 4
//! when a count fails the gate, so a caller that reads only the exit code
//! stays safe. A run with `--execute` creates `maintenance.lock` in the store
//! root before the count pass. The file stays also when the gate fails.
//!
//! A group whose newest segment ends in a torn record is refused with exit
//! code 2 before any load, because a load would cut the file. The reap never
//! cuts a segment. Repair the group, then run the reap again.
//!
//! The gate needs `expect_keys` in every rules file. The matched keys must
//! equal it. `--already-applied-ok` also accepts fewer matched keys, so a rerun
//! can finish after an earlier run or after a kill. More matched keys never
//! pass. `--home` is the store root, or the home that holds it in `data/`.
//!
//! The count line of each collection is a JSON object with the fields
//! `collection groups keys_scanned matched_keys matched_bytes expect_keys
//! expect_bytes ok`. Each other JSON line has an `event` field.

mod args;
mod output;

use laststore::{reap_home, ReapOptions};
use std::process::ExitCode;

/// Run the `reap` subcommand. `args` are the arguments after the word `reap`.
pub(crate) fn run(args: impl Iterator<Item = String>) -> ExitCode {
    let parsed = match args::parse(args) {
        Ok(args::Parsed::Run(parsed)) => parsed,
        Ok(args::Parsed::Help) => {
            eprintln!("{}", args::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("{message}");
            eprintln!("{}", args::USAGE);
            return ExitCode::from(2);
        }
    };
    let printer = output::Printer { json: parsed.json };
    let options = ReapOptions {
        execute: parsed.execute,
        already_applied_ok: parsed.already_applied_ok,
    };
    eprintln!(
        "reap home={} plan_dir={} execute={}",
        parsed.home.display(),
        parsed.plan_dir.display(),
        parsed.execute
    );
    let result = reap_home(
        &parsed.home,
        &parsed.plan_dir,
        parsed.collections.as_deref(),
        &options,
        &mut |event| printer.event(event),
    );
    match result {
        Ok(outcome) => {
            printer.summary(&outcome);
            if outcome.gate_problems.is_empty() {
                return ExitCode::SUCCESS;
            }
            for problem in &outcome.gate_problems {
                eprintln!("gate mismatch: {problem}");
            }
            ExitCode::from(4)
        }
        Err(error) => {
            printer.error(&error);
            ExitCode::from(error.exit_code())
        }
    }
}
