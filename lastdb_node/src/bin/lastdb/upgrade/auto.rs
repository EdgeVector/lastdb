//! The hourly `--auto` run of design section 12. It changes nothing outside the
//! window except a rollback of a withdrawn running build.

use lastdb_node::profile::endpoints::Environment;

use super::commands::{record, running};
use super::config::{in_window, Config};
use super::fetch::{load_list, ListError};
use super::list::NodeList;
use super::select::{pick, withdrawn_running, Mode, Running};
use super::stage::{self, StageError};
use super::state::{self, Paths};
use super::switch::{Outcome, Switch};

pub(super) fn auto(paths: &Paths, cfg: &Config, env: Environment) -> Result<Outcome, String> {
    if cfg.auto == "off" {
        return Ok(Outcome::upgraded("automatic upgrades are off."));
    }
    let Ok(run) = running(paths) else {
        return Ok(Outcome::upgraded(
            "no service is set up here; nothing to check.",
        ));
    };
    let list = match load_list(paths, env, &cfg.channel) {
        Ok(list) => list,
        Err(e @ (ListError::Unreachable(_) | ListError::NoUpdate(_))) => {
            return Ok(Outcome::upgraded(format!(
                "no update this check: {}",
                e.message()
            )));
        }
        Err(ListError::Refused(m)) => {
            let outcome = Outcome::refused(format!("the release list was refused: {m}"));
            record(paths, &run.version, "-", "auto", &outcome, "resolve");
            return Ok(outcome);
        }
    };
    if let Some(outcome) = withdrawn(paths, cfg, &list, &run)? {
        return Ok(outcome);
    }
    apply(paths, cfg, &list, &run)
}

/// The running build is withdrawn: roll back to `previous` at once, outside the window.
/// `None` when the running build is not withdrawn.
fn withdrawn(
    paths: &Paths,
    cfg: &Config,
    list: &NodeList,
    run: &Running,
) -> Result<Option<Outcome>, String> {
    let Some(entry) = withdrawn_running(list, run) else {
        return Ok(None);
    };
    let reason = entry.withdrawn_reason.clone().unwrap_or_default();
    if cfg.auto != "apply" {
        return Ok(Some(Outcome::upgraded(format!(
            "{} is withdrawn ({reason}); run 'lastdb upgrade --rollback'.",
            run.version
        ))));
    }
    let Some(previous) = paths.link_version(&paths.previous()) else {
        let outcome = Outcome::needs_human(format!(
            "{} is withdrawn ({reason}) and there is no previous pair to go back to.",
            run.version
        ));
        record(paths, &run.version, "-", "withdrawn", &outcome, "resolve");
        return Ok(Some(outcome));
    };
    if list
        .releases
        .iter()
        .any(|r| r.version == previous && !r.active())
    {
        let outcome = Outcome::needs_human(format!(
            "{} is withdrawn ({reason}) and the previous pair {previous} is withdrawn too; waiting for a human.",
            run.version
        ));
        record(
            paths,
            &run.version,
            &previous,
            "withdrawn",
            &outcome,
            "resolve",
        );
        return Ok(Some(outcome));
    }
    let label = cfg.label()?.to_owned();
    Ok(Some(
        Switch {
            paths,
            cfg,
            label: &label,
            from: run.version.clone(),
            to: previous,
            trigger: "withdrawn",
            accept_behind: false,
            adopt: false,
            reject_from: true,
        }
        .run(),
    ))
}

/// A newer active release in this install's share, inside the window.
fn apply(paths: &Paths, cfg: &Config, list: &NodeList, run: &Running) -> Result<Outcome, String> {
    let id = state::install_id(paths)?;
    let rejected = state::rejected(paths);
    let Some(release) = pick(list, &Mode::Auto, run, &id, &rejected)? else {
        return Ok(Outcome::upgraded(if run.release.is_none() {
            format!(
                "{} has no release record; automatic upgrades skip it.",
                run.version
            )
        } else {
            format!("{} is current.", run.version)
        }));
    };
    if cfg.auto == "check" {
        return Ok(Outcome::upgraded(format!(
            "{} is available; automatic upgrades only check. Run 'lastdb upgrade'.",
            release.version
        )));
    }
    if !in_window(&cfg.window)? {
        return Ok(Outcome::upgraded(format!(
            "{} waits for the window {}.",
            release.version, cfg.window
        )));
    }
    let label = cfg.label()?.to_owned();
    let staged = match stage::stage_release(paths, release, &paths.home) {
        Ok(staged) => staged,
        Err(StageError::Transient(m)) => {
            return Ok(Outcome::upgraded(format!(
                "the download failed and will be tried again: {m}"
            )));
        }
        Err(StageError::Refused(m)) => {
            // One failure per version: automatic runs do not try it again.
            let _ = state::add_rejected(paths, &release.version);
            let outcome = Outcome::refused(format!(
                "{} was refused and is now rejected: {m}",
                release.version
            ));
            record(
                paths,
                &run.version,
                &release.version,
                "auto",
                &outcome,
                "download",
            );
            return Ok(outcome);
        }
    };
    Ok(Switch {
        paths,
        cfg,
        label: &label,
        from: run.version.clone(),
        to: staged.version,
        trigger: "auto",
        accept_behind: false,
        adopt: false,
        reject_from: false,
    }
    .run())
}
