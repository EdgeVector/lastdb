//! Command line of `lastdb-maintenance reap`.

use std::path::PathBuf;

pub(super) const USAGE: &str = "usage: lastdb-maintenance reap --home <path> --plan-dir <dir> \
[--collections a,b,c] [--execute] [--already-applied-ok] [--json]";

/// What the command line asks for.
pub(super) struct ReapArgs {
    pub home: PathBuf,
    pub plan_dir: PathBuf,
    pub collections: Option<Vec<String>>,
    pub execute: bool,
    pub already_applied_ok: bool,
    pub json: bool,
}

/// The result of reading the command line.
pub(super) enum Parsed {
    Run(ReapArgs),
    Help,
}

fn value_of(flag: &str, next: Option<String>) -> Result<String, String> {
    next.ok_or_else(|| format!("{flag} needs a value"))
}

fn split_collections(list: &str) -> Result<Vec<String>, String> {
    let names: Vec<String> = list.split(',').map(str::to_string).collect();
    if names.iter().any(String::is_empty) {
        return Err("--collections has an empty name".to_string());
    }
    Ok(names)
}

/// Read the arguments after the `reap` word.
pub(super) fn parse(args: impl Iterator<Item = String>) -> Result<Parsed, String> {
    let mut home = None;
    let mut plan_dir = None;
    let mut collections = None;
    let mut execute = false;
    let mut already_applied_ok = false;
    let mut json = false;
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(Parsed::Help),
            "--execute" => execute = true,
            "--already-applied-ok" => already_applied_ok = true,
            "--json" => json = true,
            "--home" => home = Some(PathBuf::from(value_of("--home", args.next())?)),
            "--plan-dir" => plan_dir = Some(PathBuf::from(value_of("--plan-dir", args.next())?)),
            "--collections" => {
                collections = Some(split_collections(&value_of("--collections", args.next())?)?);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(Parsed::Run(ReapArgs {
        home: home.ok_or("--home is required")?,
        plan_dir: plan_dir.ok_or("--plan-dir is required")?,
        collections,
        execute,
        already_applied_ok,
        json,
    }))
}
