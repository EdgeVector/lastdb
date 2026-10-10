//! `lastdb_local_maintain reap`: the offline planner for the dropped-schema reap.
//!
//! Window 1 has two verbs:
//!
//! - `reap plan` reads a stopped home and writes a plan directory (contract v1).
//!   It never writes to the home. Rules for the engine list the keys to drop.
//!   Each rules file also states how many keys and bytes it must match.
//!   A doomed tip that is newer than the drop of its molecule stops the plan
//!   (the post-drop tripwire, `--tripwire-slack-ms`).
//! - `reap sizing` prints the sizing of a finished plan directory.
//!
//! Exit codes: 0 done, 2 usage or guard refusal (no store read), 3 a plan abort
//! rule fired (the home is unchanged), 1 any other failure.

use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};

mod catalog;
mod count_pass;
mod errors;
mod guard;
mod identities;
mod keys;
mod meters;
mod molset;
mod plan;
mod plan_file;
mod plan_report;
mod plan_rules;
mod proteins;
mod receipts;
mod rules;
mod rules_out;
mod sizing;
mod tips_pass;
mod tripwire;
mod walk;

pub(crate) use errors::ReapError;

/// Arguments of `reap`.
#[derive(Args, Debug)]
pub(crate) struct ReapArgs {
    #[command(subcommand)]
    cmd: ReapCmd,
}

#[derive(Subcommand, Debug)]
enum ReapCmd {
    /// Plan the reap of the dropped schemas (read-only).
    ///
    /// Reads the home, proves that no daemon serves it, and writes the plan
    /// directory: rules per collection, retained.tsv and plan.json.
    Plan(PlanArgs),
    /// Print the sizing of a finished plan directory.
    Sizing(SizingArgs),
}

#[derive(Args, Debug)]
struct PlanArgs {
    /// File with the dropped names, one per line.
    #[arg(long)]
    identities: PathBuf,
    /// New or empty directory for the plan. It must be outside the home.
    #[arg(long)]
    plan_dir: PathBuf,
    /// The daemon of this home is stopped. Needed for a home under ~/.lastdb or ~/.folddb.
    #[arg(long)]
    stopped_primary: bool,
    /// Confirms that the home is a primary home. Needed with --stopped-primary.
    #[arg(long)]
    i_know_this_is_primary: bool,
    /// Reader count. The planner reads with one worker, so a larger value has no effect.
    #[arg(long, default_value_t = 1)]
    workers: usize,
    /// Slack of the post-drop tripwire in milliseconds. A doomed tip written later
    /// than its drop time plus this slack stops the plan.
    #[arg(long, default_value_t = tripwire::DEFAULT_SLACK_MS)]
    tripwire_slack_ms: u64,
    /// Print plan.json instead of the sizing summary.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct SizingArgs {
    /// A finished plan directory.
    #[arg(long)]
    plan_dir: PathBuf,
    #[arg(long)]
    json: bool,
}

/// Run a `reap` verb. A guard refusal or a plan abort ends the process with
/// its own exit code. Any other failure returns an error text.
pub(crate) fn run(home: &Path, args: ReapArgs) -> Result<(), String> {
    let result = match args.cmd {
        ReapCmd::Plan(plan_args) => run_plan_verb(home, &plan_args),
        ReapCmd::Sizing(sizing_args) => sizing::run(&sizing_args.plan_dir, sizing_args.json),
    };
    match result {
        Ok(()) => Ok(()),
        Err(ReapError::Failed(message)) => Err(message),
        Err(error) => {
            eprintln!("lastdb_local_maintain reap: {error}");
            std::process::exit(error.exit_code());
        }
    }
}

/// The planner inputs that the flags of `reap plan` give.
fn plan_run<'a>(home: &'a Path, args: &'a PlanArgs) -> plan::PlanRun<'a> {
    plan::PlanRun {
        home,
        identities: &args.identities,
        plan_dir: &args.plan_dir,
        flags: guard::Flags {
            stopped_primary: args.stopped_primary,
            i_know_this_is_primary: args.i_know_this_is_primary,
        },
        workers: args.workers,
        tripwire_slack_ms: args.tripwire_slack_ms,
        view: None,
    }
}

fn run_plan_verb(home: &Path, args: &PlanArgs) -> Result<(), ReapError> {
    let plan = plan::run_plan(&plan_run(home, args))?;
    if args.json {
        let text = serde_json::to_string_pretty(&plan)
            .map_err(|error| ReapError::Failed(format!("plan.json: {error}")))?;
        println!("{text}");
    } else {
        print!("{}", sizing::render(&plan));
    }
    Ok(())
}
