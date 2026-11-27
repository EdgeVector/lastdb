//! The verbs of `lastdb upgrade`: design section 8 step 1 (lock and resolve) and the
//! hourly `--auto` run of section 12.

use std::path::PathBuf;

use lastdb_node::profile::endpoints::Environment;
use serde_json::json;

use super::args::UpgradeArgs;
use super::config::Config;
use super::fetch::load_list;
use super::select::{pick, Mode, Running};
use super::stage::{self, read_release};
use super::state::{self, acquire_lock, Attempt, LockError, Paths};
use super::status;
use super::switch::{note, Outcome, Switch};

fn busy() -> Outcome {
    Outcome {
        code: 5,
        result: "busy",
        message: "another upgrade holds the lock; nothing changed.".into(),
    }
}

pub(super) fn running(paths: &Paths) -> Result<Running, String> {
    let version = paths.link_version(&paths.current()).ok_or_else(|| {
        "current does not name a program folder under programs/; run 'lastdb service install' or 'lastdb service adopt --label <label>' first"
            .to_string()
    })?;
    let release = read_release(&paths.program(&version));
    Ok(Running { version, release })
}

/// Write one history line for an attempt that never reached the switch.
pub(super) fn record(
    paths: &Paths,
    from: &str,
    to: &str,
    trigger: &'static str,
    outcome: &Outcome,
    step: &str,
) {
    let attempt = Attempt::begin(from, to, trigger);
    if let Err(e) = attempt.finish(paths, outcome.result, step, &outcome.message) {
        note(&format!("cannot write history: {e}"));
    }
}

fn check_flags(args: &UpgradeArgs) -> Result<(), String> {
    let verbs = [
        args.status,
        args.auto,
        args.rollback,
        args.clear_rejected.is_some(),
        args.pair.is_some(),
    ];
    let named = args.target_version.is_some() || args.channel.is_some();
    if verbs.iter().filter(|v| **v).count() > 1
        || (verbs.iter().any(|v| *v) && (named || args.check))
    {
        return Err("choose one of --status, --auto, --rollback, --clear-rejected and --pair; --check and --version or --channel go together".into());
    }
    if args.target_version.is_some() && args.channel.is_some() {
        return Err("--version and --channel cannot be used together".into());
    }
    Ok(())
}

pub(crate) fn run_upgrade(data_dir: Option<PathBuf>, args: &UpgradeArgs) -> Result<(), String> {
    check_flags(args)?;
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let paths = Paths::new(&home);
    let outcome = match dispatch(&paths, args) {
        Ok(outcome) => outcome,
        Err(e) => Outcome::refused(e),
    };
    emit(args.json, &outcome)
}

/// Print the one sentence (or the JSON object) and leave with the exit code.
pub(super) fn emit(json: bool, outcome: &Outcome) -> Result<(), String> {
    if json {
        println!(
            "{}",
            json!({"exit_code": outcome.code, "result": outcome.result, "message": outcome.message})
        );
    } else if !outcome.message.is_empty() {
        println!("{}", outcome.message);
    }
    if outcome.code != 0 {
        std::process::exit(outcome.code);
    }
    Ok(())
}

fn dispatch(paths: &Paths, args: &UpgradeArgs) -> Result<Outcome, String> {
    let (mut cfg, configured) = Config::load(paths)?;
    if args.status {
        let report = status::report(paths, &cfg, configured);
        if args.json {
            println!("{report}");
        } else {
            status::print_report(&report);
        }
        return Ok(Outcome::upgraded(""));
    }
    let _lock = match acquire_lock(paths) {
        Ok(lock) => lock,
        Err(LockError::Busy) => return Ok(busy()),
        Err(LockError::Other(e)) => return Err(e),
    };
    let env = Environment::from_env_or_prod();
    if let Some(version) = &args.clear_rejected {
        let cleared = state::clear_rejected(paths, version)?;
        return Ok(Outcome::upgraded(if cleared {
            format!("{version} is no longer rejected.")
        } else {
            format!("{version} was not rejected.")
        }));
    }
    if args.auto {
        return super::auto::auto(paths, &cfg, env);
    }
    if args.rollback {
        return rollback(paths, &cfg, args.accept_backup_behind);
    }
    let run = running(paths)?;
    if let Some(dir) = &args.pair {
        return pair(paths, &cfg, &run, dir, args.accept_backup_behind);
    }
    release_flow(paths, &mut cfg, args, env, &run)
}

/// The default verb, `--check`, `--version` and `--channel`: resolve an entry of the
/// list, stage it, and switch.
fn release_flow(
    paths: &Paths,
    cfg: &mut Config,
    args: &UpgradeArgs,
    env: Environment,
    run: &Running,
) -> Result<Outcome, String> {
    let channel = args.channel.clone().unwrap_or_else(|| cfg.channel.clone());
    let list = match load_list(paths, env, &channel) {
        Ok(list) => list,
        Err(e) => {
            return Ok(Outcome::refused(format!(
                "no release list could be used: {}",
                e.message()
            )))
        }
    };
    let mode = match (&args.target_version, &args.channel) {
        (Some(v), _) => Mode::Named(Some(v.clone())),
        (None, Some(_)) => Mode::Named(None),
        (None, None) => Mode::Manual,
    };
    let id = state::install_id(paths)?;
    let rejected = state::rejected(paths);
    let Some(release) = pick(&list, &mode, run, &id, &rejected)? else {
        return Ok(Outcome::upgraded(format!(
            "no newer release is available on the {channel} channel; {} stays.",
            run.version
        )));
    };
    if release.version == run.version {
        return Ok(Outcome::upgraded(format!(
            "{} is already current.",
            run.version
        )));
    }
    if args.check {
        let share = if super::select::in_share(&id, release) {
            "inside"
        } else {
            "outside"
        };
        return Ok(Outcome::upgraded(format!(
            "would install {} ({channel}, published {}, rollout {}%, this install is {share} the share); it runs now: {}.",
            release.version, release.published_at, release.rollout.percent, run.version
        )));
    }
    if args.channel.is_some() && cfg.channel != channel {
        cfg.channel.clone_from(&channel);
        cfg.save(paths)?;
    }
    let label = cfg.label()?.to_owned();
    let staged = match stage::stage_release(paths, release, &paths.home) {
        Ok(staged) => staged,
        Err(e) => {
            let outcome = Outcome::refused(e.message().to_owned());
            record(
                paths,
                &run.version,
                &release.version,
                "manual",
                &outcome,
                "download",
            );
            return Ok(outcome);
        }
    };
    let outcome = Switch {
        paths,
        cfg,
        label: &label,
        from: run.version.clone(),
        to: staged.version,
        trigger: "manual",
        accept_behind: args.accept_backup_behind,
        adopt: false,
        reject_from: false,
    }
    .run();
    if outcome.code == 0 {
        // A person who named a version that was rejected has now taken it.
        let _ = state::clear_rejected(paths, &release.version);
    }
    Ok(outcome)
}

fn pair(
    paths: &Paths,
    cfg: &Config,
    run: &Running,
    dir: &std::path::Path,
    accept_behind: bool,
) -> Result<Outcome, String> {
    let source =
        std::fs::canonicalize(dir).map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
    let label = cfg.label()?.to_owned();
    let staged = match stage::stage_pair(paths, &source, &paths.home) {
        Ok(staged) => staged,
        Err(e) => {
            let outcome = Outcome::refused(e.message().to_owned());
            record(paths, &run.version, "pair", "manual", &outcome, "download");
            return Ok(outcome);
        }
    };
    if staged.version == run.version {
        return Ok(Outcome::upgraded(format!(
            "{} is already current.",
            run.version
        )));
    }
    Ok(Switch {
        paths,
        cfg,
        label: &label,
        from: run.version.clone(),
        to: staged.version,
        trigger: "manual",
        accept_behind,
        adopt: false,
        reject_from: false,
    }
    .run())
}

fn rollback(paths: &Paths, cfg: &Config, accept_behind: bool) -> Result<Outcome, String> {
    let run = running(paths)?;
    let label = cfg.label()?.to_owned();
    let Some(previous) = paths.link_version(&paths.previous()) else {
        return Ok(Outcome::refused(
            "there is no previous program pair to go back to.",
        ));
    };
    let dir = paths.program(&previous);
    for name in ["lastdb", "lastdbd"] {
        if !dir.join(name).is_file() {
            return Ok(Outcome::refused(format!(
                "programs/{previous} holds no {name}."
            )));
        }
    }
    if let Err(e) = super::checks::check_home_format(&dir, &paths.home) {
        return Ok(Outcome::refused(e.message().to_owned()));
    }
    Ok(Switch {
        paths,
        cfg,
        label: &label,
        from: run.version,
        to: previous,
        trigger: "manual",
        accept_behind,
        adopt: false,
        reject_from: true,
    }
    .run())
}
