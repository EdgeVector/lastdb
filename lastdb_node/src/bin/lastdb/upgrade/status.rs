//! `lastdb upgrade --status` and the `Upgrade:` line of `lastdb status`. Both read only
//! local files: the installed pairs, the config, the history and the last verified list.

use std::path::Path;

use lastdb_node::profile::endpoints::Environment;
use serde_json::{json, Value};

use super::config::Config;
use super::fetch::cached_list;
use super::list::{parse_time, NodeList};
use super::select::{in_share, is_newer, withdrawn_running, Running};
use super::stage::read_release;
use super::state::{self, Paths};

fn running_of(paths: &Paths) -> Option<Running> {
    let version = paths.link_version(&paths.current())?;
    let release = read_release(&paths.program(&version));
    Some(Running { version, release })
}

fn local_time(stamp: &str) -> String {
    parse_time(stamp).map_or_else(
        || stamp.to_owned(),
        |t| t.with_timezone(&chrono::Local).format("%H:%M").to_string(),
    )
}

/// The newest active release that is newer than the running pair and not rejected.
fn newest_candidate<'a>(
    list: &'a NodeList,
    running: &Running,
    rejected: &[String],
) -> Option<&'a super::list::Release> {
    list.releases
        .iter()
        .find(|r| r.active() && is_newer(r, running) && !rejected.contains(&r.version))
}

/// The one `Upgrade:` line.
pub(crate) fn status_line(home: &Path) -> String {
    let paths = Paths::new(home);
    let Some(running) = running_of(&paths) else {
        return "Upgrade: not set up (run 'lastdb service install', or 'lastdb service adopt --label <label>' for an existing job)".into();
    };
    let (cfg, _) = Config::load(&paths).unwrap_or_else(|_| (Config::defaults(), false));
    if let Some(last) = state::last_history(&paths) {
        let result = last["result"].as_str().unwrap_or("");
        if matches!(result, "rolled-back" | "needs-human") {
            let at = last["finished_at"]
                .as_str()
                .map_or_else(String::new, |t| format!(" at {}", local_time(t)));
            let from = last["to"].as_str().unwrap_or("?");
            let why = last["step"].as_str().unwrap_or("");
            let word = if result == "rolled-back" {
                "rolled back from"
            } else {
                "needs a human after trying"
            };
            return format!(
                "Upgrade: {word} {from}{at} ({why}: {})",
                first_sentence(last["detail"].as_str().unwrap_or(""))
            );
        }
    }
    let Some(release) = &running.release else {
        return format!(
            "Upgrade: {} has no release record (installed with --pair or before updates); automatic upgrades skip it",
            running.version
        );
    };
    let env = Environment::from_env_or_prod();
    let list = cached_list(&paths, env, &cfg.channel);
    let rejected = state::rejected(&paths);
    if let Some(list) = &list {
        if let Some(w) = withdrawn_running(list, &running) {
            let reason = w.withdrawn_reason.as_deref().unwrap_or("no reason given");
            let next = if cfg.auto == "apply" {
                "it rolls back at the next hourly check"
            } else {
                "run 'lastdb upgrade --rollback'"
            };
            return format!(
                "Upgrade: {} is withdrawn ({reason}); {next}",
                running.version
            );
        }
        if let Some(candidate) = newest_candidate(list, &running, &rejected) {
            let percent = candidate.rollout.percent;
            let id = state::read_install_id(&paths).unwrap_or_default();
            if !id.is_empty() && in_share(&id, candidate) {
                return match cfg.auto.as_str() {
                    "apply" => format!(
                        "Upgrade: {} installs in the window {} ({}, rollout {percent}%)",
                        candidate.version, cfg.window, cfg.channel
                    ),
                    _ => format!(
                        "Upgrade: {} is available ({}, rollout {percent}%); run 'lastdb upgrade'",
                        candidate.version, cfg.channel
                    ),
                };
            }
            return format!(
                "Upgrade: {} is rolling out ({percent}%) and has not reached this install yet ({})",
                candidate.version, cfg.channel
            );
        }
    }
    let note = if cfg.auto == "off" {
        "; automatic upgrades are off"
    } else {
        ""
    };
    let published = local_date(&release.published_at);
    format!(
        "Upgrade: {} is current ({}, published {published}){note}",
        running.version, cfg.channel
    )
}

fn local_date(stamp: &str) -> String {
    parse_time(stamp).map_or_else(
        || stamp.to_owned(),
        |t| {
            t.with_timezone(&chrono::Local)
                .format("%Y-%m-%d")
                .to_string()
        },
    )
}

fn first_sentence(text: &str) -> String {
    let line = text.lines().next().unwrap_or("");
    let cut = line.find(". ").map_or(line.len(), |i| i + 1);
    line[..cut].trim().to_owned()
}

/// The full `--status` report.
pub(super) fn report(paths: &Paths, cfg: &Config, configured: bool) -> Value {
    let running = running_of(paths);
    let previous = paths.link_version(&paths.previous());
    let env = Environment::from_env_or_prod();
    let rejected = state::rejected(paths);
    let list = cached_list(paths, env, &cfg.channel);
    let available = match (&list, &running) {
        (Some(list), Some(r)) => newest_candidate(list, r, &rejected).map(|c| {
            json!({
                "version": c.version, "published_at": c.published_at,
                "rollout_percent": c.rollout.percent,
                "in_share": state::read_install_id(paths).is_some_and(|id| in_share(&id, c)),
            })
        }),
        _ => None,
    };
    json!({
        "home": paths.home.display().to_string(),
        "current": running.as_ref().map(|r| r.version.clone()),
        "release_record": running.as_ref().map(|r| r.release.is_some()),
        "previous": previous,
        "configured": configured,
        "channel": cfg.channel, "auto": cfg.auto, "window": cfg.window,
        "service_label": cfg.service_label,
        "list_seq": list.as_ref().map(|l| l.seq),
        "list_generated_at": list.as_ref().map(|l| l.generated_at.clone()),
        "available": available,
        "last_attempt": state::last_history(paths),
        "rejected": rejected,
        "summary": status_line(&paths.home),
    })
}

pub(super) fn print_report(v: &Value) {
    let text = |key: &str| v[key].as_str().unwrap_or("-").to_owned();
    println!(
        "Current:  {} (release record: {})",
        text("current"),
        if v["release_record"].as_bool() == Some(true) {
            "yes"
        } else {
            "no"
        }
    );
    println!("Previous: {}", text("previous"));
    println!(
        "Channel:  {}   Auto: {}   Window: {}",
        text("channel"),
        text("auto"),
        text("window")
    );
    println!("Service:  {}", text("service_label"));
    match v["available"].as_object() {
        Some(a) => println!(
            "Available: {} (rollout {}%, this install {})",
            a["version"].as_str().unwrap_or("?"),
            a["rollout_percent"],
            if a["in_share"].as_bool() == Some(true) {
                "is in the share"
            } else {
                "is not in the share yet"
            }
        ),
        None => println!("Available: nothing newer in the last verified list"),
    }
    match v["last_attempt"].as_object() {
        Some(l) => println!(
            "Last attempt: {} -> {} ({}) {} at step {}: {}",
            l["from"].as_str().unwrap_or("?"),
            l["to"].as_str().unwrap_or("?"),
            l["trigger"].as_str().unwrap_or("?"),
            l["result"].as_str().unwrap_or("?"),
            l["step"].as_str().unwrap_or("?"),
            l["detail"].as_str().unwrap_or("")
        ),
        None => println!("Last attempt: none"),
    }
    let rejected: Vec<&str> = v["rejected"]
        .as_array()
        .map_or_else(Vec::new, |a| a.iter().filter_map(Value::as_str).collect());
    println!(
        "Rejected: {}",
        if rejected.is_empty() {
            "none".to_owned()
        } else {
            rejected.join(", ")
        }
    );
    println!("{}", text("summary"));
}
