//! `lastdb upgrade` and `lastdb service`: LastDB updates itself. The formats and the
//! exit codes are `docs/designs/node-self-update.md`; the steps are section 8 of the
//! design. This module owns "switch this host safely". The release pipeline owns
//! "is this build good", and the daemon owns "can this build open this home".

mod args;
mod auto;
mod checks;
mod commands;
mod config;
mod daemon;
mod fetch;
mod list;
mod procs;
mod select;
mod service;
mod service_cmds;
mod stage;
mod state;
mod status;
mod switch;
mod switch_post;
mod switch_pre;
mod switch_rollback;

pub(crate) use args::{ServiceCommand, UpgradeArgs};
pub(crate) use commands::run_upgrade;
pub(crate) use service_cmds::run_service;
pub(crate) use status::status_line;
