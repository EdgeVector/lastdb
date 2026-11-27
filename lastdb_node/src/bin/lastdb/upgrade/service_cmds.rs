//! `lastdb service install` and `lastdb service adopt`: contract section 11.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lastdb_node::profile::endpoints::Environment;

use super::args::ServiceCommand;
use super::commands::emit;
use super::config::{valid_label, Config};
use super::daemon;
use super::fetch::{cached_list, load_list};
use super::procs;
use super::service;
use super::stage::{self, Staged};
use super::state::{self, acquire_lock, atomic_symlink, LockError, Paths, DEFAULT_LABEL};
use super::switch::{note, poll, Outcome, Switch};

pub(crate) fn run_service(
    data_dir: Option<PathBuf>,
    command: &ServiceCommand,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let paths = Paths::new(&home);
    let _lock = match acquire_lock(&paths) {
        Ok(lock) => lock,
        Err(LockError::Busy) => {
            let busy = Outcome {
                code: 5,
                result: "busy",
                message: "another upgrade holds the lock; nothing changed.".into(),
            };
            let json = matches!(
                command,
                ServiceCommand::Install { json: true } | ServiceCommand::Adopt { json: true, .. }
            );
            return emit(json, &busy);
        }
        Err(LockError::Other(e)) => return Err(e),
    };
    let (json, outcome) = match command {
        ServiceCommand::Install { json } => (*json, install(&paths)),
        ServiceCommand::Adopt {
            label,
            accept_backup_behind,
            json,
        } => (*json, adopt(&paths, label, *accept_backup_behind)),
    };
    emit(json, &outcome.unwrap_or_else(Outcome::refused))
}

/// The folder that holds this `lastdb` and its `lastdbd`.
fn installer_dir() -> Result<PathBuf, String> {
    let exe = std::env::current_exe()
        .and_then(fs::canonicalize)
        .map_err(|e| format!("cannot find this program: {e}"))?;
    let dir = exe
        .parent()
        .ok_or("this program has no folder")?
        .to_path_buf();
    if dir.join("lastdbd").is_file() && dir.join("lastdb").is_file() {
        Ok(dir)
    } else {
        Err(format!(
            "{} does not hold both lastdb and lastdbd; run this command from the unpacked release",
            dir.display()
        ))
    }
}

/// Keep the list entry of an installed pair when a verified list names exactly these
/// binaries. A pair that is not in a list stays without a record.
fn attach_release_record(paths: &Paths, staged: &Staged, env: Environment, channel: &str) {
    let list = cached_list(paths, env, channel).or_else(|| load_list(paths, env, channel).ok());
    let Some(list) = list else {
        return;
    };
    let Some(release) = list.releases.iter().find(|r| r.version == staged.version) else {
        return;
    };
    if let Err(e) = stage::attach_release(&staged.dir, release) {
        note(&format!(
            "no release record kept for {}: {e}",
            staged.version
        ));
    }
}

fn map_stage(e: &stage::StageError) -> Outcome {
    Outcome::refused(e.message().to_owned())
}

/// Make `~/.local/bin/lastdb` a link to `<home>/current/lastdb`. A regular file there is
/// left alone.
fn link_cli(home: &Path) -> Result<(), String> {
    let bin = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is required")?).join(".local/bin");
    fs::create_dir_all(&bin).map_err(|e| format!("cannot create {}: {e}", bin.display()))?;
    let link = bin.join("lastdb");
    if let Ok(meta) = fs::symlink_metadata(&link) {
        if !meta.file_type().is_symlink() {
            note(&format!(
                "{} is a regular file; it was not replaced",
                link.display()
            ));
            return Ok(());
        }
    }
    atomic_symlink(&link, &home.join("current/lastdb"))
}

/// The hourly update check: on macOS the app-registry LaunchAgent, which also runs
/// `lastdb upgrade --auto`; on Linux a systemd user timer.
fn install_update_check(home: &Path) -> Result<(), String> {
    if service::is_mac() {
        return crate::app_registry_index::managed::install_poll();
    }
    let dir = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is required")?)
        .join(".config/systemd/user");
    let h = home.display().to_string();
    let unit = format!(
        "[Unit]\nDescription=LastDB update check\n\n[Service]\nType=oneshot\nEnvironment=LASTDB_HOME={h}\nExecStart={h}/current/lastdb upgrade --auto\n"
    );
    let timer = "[Unit]\nDescription=LastDB update check, hourly\n\n[Timer]\nOnBootSec=10min\nOnUnitActiveSec=1h\n\n[Install]\nWantedBy=timers.target\n";
    state::atomic_write(&dir.join("lastdb-upgrade.service"), unit.as_bytes(), 0o644)?;
    state::atomic_write(&dir.join("lastdb-upgrade.timer"), timer.as_bytes(), 0o644)?;
    for args in [
        &["--user", "daemon-reload"][..],
        &["--user", "enable", "--now", "lastdb-upgrade.timer"][..],
    ] {
        let out = procs::run(Path::new("systemctl"), args, Duration::from_secs(60))?;
        if !out.ok {
            return Err(format!(
                "systemctl {}: {}",
                args.join(" "),
                out.stderr.trim()
            ));
        }
    }
    Ok(())
}

fn install(paths: &Paths) -> Result<Outcome, String> {
    let home = &paths.home;
    let (mut cfg, _) = Config::load(paths)?;
    let label = cfg
        .service_label
        .clone()
        .unwrap_or_else(|| DEFAULT_LABEL.to_owned());
    let definition = service::definition_path(&label)?;
    if definition.exists() {
        return Ok(Outcome::refused(format!(
            "{} already exists. For an existing job run 'lastdb service adopt --label {label}'.",
            definition.display()
        )));
    }
    if daemon::health_ok(&paths.socket()) {
        return Ok(Outcome::refused(
            "a daemon already serves this home. Run 'lastdb service adopt --label <label>' for its job.",
        ));
    }
    if fs::symlink_metadata(paths.current()).is_ok() {
        return Ok(Outcome::refused(format!(
            "{} already exists; this home is not fresh.",
            paths.current().display()
        )));
    }
    let staged = match stage::stage_pair(paths, &installer_dir()?, home) {
        Ok(staged) => staged,
        Err(e) => return Ok(map_stage(&e)),
    };
    attach_release_record(
        paths,
        &staged,
        Environment::from_env_or_prod(),
        &cfg.channel,
    );
    atomic_symlink(
        &paths.current(),
        &PathBuf::from("programs").join(&staged.version),
    )?;
    cfg.service_label = Some(label.clone());
    cfg.save(paths)?;
    service::write_definition(&label, home, cfg.stop_timeout_secs)?;
    link_cli(home)?;
    note("starting the service");
    service::start(&label)?;
    let socket = paths.socket();
    let up = poll(Duration::from_secs(120), Duration::from_secs(2), || {
        if daemon::health_ok(&socket) {
            Ok(())
        } else {
            Err("the daemon does not answer /health".to_string())
        }
    });
    if let Err(e) = up {
        return Ok(Outcome::needs_human(format!(
            "the service {label} started but {e}. See {}/logs. Nothing was removed.",
            home.display()
        )));
    }
    install_update_check(home)?;
    Ok(Outcome::upgraded(format!(
        "installed {label}: lastdbd {} runs from {}/current.",
        staged.version,
        home.display()
    )))
}

fn adopt(paths: &Paths, label: &str, accept_behind: bool) -> Result<Outcome, String> {
    let home = &paths.home;
    if !valid_label(label) {
        return Ok(Outcome::refused(
            "the label has characters a job label cannot hold.",
        ));
    }
    let definition = service::definition_path(label)?;
    if !definition.exists() {
        return Ok(Outcome::refused(format!(
            "no job {label} exists at {}.",
            definition.display()
        )));
    }
    if let Some(job_home) = service::home_of(label) {
        let same = fs::canonicalize(&job_home).ok() == fs::canonicalize(home).ok();
        if !same {
            return Ok(Outcome::refused(format!(
                "the job {label} runs the home {}, not {}.",
                job_home.display(),
                home.display()
            )));
        }
    }
    if service::program_of(label)? == home.join("current/lastdbd") {
        return Ok(Outcome::refused(format!(
            "{label} already runs {}/current/lastdbd. Use 'lastdb upgrade'.",
            home.display()
        )));
    }
    if let Ok(meta) = fs::symlink_metadata(paths.current()) {
        if !meta.file_type().is_symlink() {
            return Ok(Outcome::refused(format!(
                "{} is a real folder, not a link. Adopt moves nothing inside the home; move it aside first.",
                paths.current().display()
            )));
        }
    }
    let (_, _, build) = match daemon::boot_identity(&paths.socket()) {
        Ok(identity) => identity,
        Err(e) => {
            return Ok(Outcome::refused(format!(
                "the daemon must run to be adopted: {e}"
            )));
        }
    };
    let from = build.split_whitespace().next().unwrap_or("").to_owned();
    if !state::valid_version_dir(&from) {
        return Ok(Outcome::refused(
            "the running daemon reports a build that cannot name a folder.",
        ));
    }
    let (mut cfg, _) = Config::load(paths)?;
    let staged = match stage::stage_pair(paths, &installer_dir()?, home) {
        Ok(staged) => staged,
        Err(e) => return Ok(map_stage(&e)),
    };
    attach_release_record(
        paths,
        &staged,
        Environment::from_env_or_prod(),
        &cfg.channel,
    );
    let outcome = Switch {
        paths,
        cfg: &cfg,
        label,
        from,
        to: staged.version.clone(),
        trigger: "manual",
        accept_behind,
        adopt: true,
        reject_from: false,
    }
    .run();
    if outcome.code != 0 {
        return Ok(outcome);
    }
    cfg.service_label = Some(label.to_owned());
    cfg.save(paths)?;
    link_cli(home)?;
    install_update_check(home)?;
    Ok(outcome)
}
