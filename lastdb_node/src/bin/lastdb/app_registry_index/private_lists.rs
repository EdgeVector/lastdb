//! Mode-0600 store of owner private registry list prefixes, keyed by `<env>:<account>`.
//!
//! Install receipts and CLI output record non-secret `private:<env>:<account>` locators
//! instead of persisting raw `/v1/lists/private/<32-hex>` URLs.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::PathBuf;

use lastdb_node::offline_home::read_private_named;
use lastdb_node::profile::endpoints::{registry_index_url_for, Environment};
use lastdb_node::profile::private_fs::write_private;
use serde::{Deserialize, Serialize};

use super::{managed, str_err};

const STORE_FILE: &str = "private-lists.json";
const LOCK_FILE: &str = "private-lists.lock";
const MAX_STORE_BYTES: u64 = 65_536;
const PRIV_ERR: &str = "saved private registry lists file must be a private 0600 owner file";
const LIM_ERR: &str = "saved private registry lists file exceeds size limit";

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredPrivateLists {
    version: u32,
    lists: BTreeMap<String, String>,
}

#[must_use]
pub(crate) fn valid_private_prefix(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn valid_owner_account(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

fn format_locator(env: Environment, account: &str) -> String {
    format!("private:{env}:{account}")
}

#[must_use]
pub(crate) fn private_index_url(env: Environment, prefix: &str) -> String {
    format!(
        "{}/private/{prefix}",
        registry_index_url_for(env).trim_end_matches('/')
    )
}

fn store_path() -> Result<PathBuf, String> {
    Ok(managed::apps_root()?.join(STORE_FILE))
}

fn lock_store() -> Result<File, String> {
    let root = managed::apps_root()?;
    fs::create_dir_all(&root).map_err(str_err)?;
    let mut opts = File::options();
    opts.read(true).write(true).create(true).truncate(false);
    let file = opts.open(root.join(LOCK_FILE)).map_err(str_err)?;
    file.lock().map_err(str_err)?;
    Ok(file)
}

fn load_map() -> Result<BTreeMap<(Environment, String), String>, String> {
    let path = store_path()?;
    let Some(raw) = read_private_named(&path, MAX_STORE_BYTES, PRIV_ERR, LIM_ERR)
        .map_err(|err| format!("{err} ({})", path.display()))?
    else {
        return Ok(BTreeMap::new());
    };
    let parsed: StoredPrivateLists = serde_json::from_slice(&raw)
        .map_err(|e| format!("invalid saved private registry lists file: {e}"))?;
    if parsed.version != 1 {
        return Err(format!(
            "unsupported saved private registry lists version {}",
            parsed.version
        ));
    }
    let mut map = BTreeMap::new();
    for (key, prefix) in parsed.lists {
        let Some((env_raw, account)) = key.split_once(':') else {
            return Err("invalid key in saved private registry lists file".into());
        };
        let env = Environment::try_from_str(env_raw)
            .map_err(|_| "invalid environment in saved private registry lists file".to_owned())?;
        if !valid_owner_account(account) || !valid_private_prefix(&prefix) {
            return Err("invalid entry in saved private registry lists file".into());
        }
        map.insert((env, account.to_owned()), prefix);
    }
    Ok(map)
}

pub(crate) fn save(env: Environment, account: &str, prefix: &str) -> Result<String, String> {
    if !valid_owner_account(account) {
        return Err("invalid registry owner account identifier".into());
    }
    if !valid_private_prefix(prefix) {
        return Err("invalid 32-hex private registry list prefix".into());
    }
    let _lock = lock_store()?;
    let mut map = load_map()?;
    map.insert((env, account.to_owned()), prefix.to_owned());
    let mut lists = BTreeMap::new();
    for ((entry_env, entry_account), entry_prefix) in map {
        lists.insert(format!("{entry_env}:{entry_account}"), entry_prefix);
    }
    let encoded =
        serde_json::to_vec_pretty(&StoredPrivateLists { version: 1, lists }).map_err(str_err)?;
    write_private(&store_path()?, &encoded).map_err(str_err)?;
    Ok(format_locator(env, account))
}

pub(super) fn resolve_locator(
    raw: &str,
    default_env: Environment,
) -> Result<(Environment, String, String), String> {
    const SYNTAX: &str =
        "invalid private list locator (expected 'private', 'private:<dev|prod>', or 'private:<dev|prod>:<account>')";
    let map = load_map()?;
    if raw == "private" {
        return resolve_for_env(&map, default_env);
    }
    let rest = raw.strip_prefix("private:").ok_or(SYNTAX)?;
    if let Some((env_raw, account)) = rest.split_once(':') {
        let env = Environment::try_from_str(env_raw).map_err(|_| SYNTAX.to_owned())?;
        if !valid_owner_account(account) {
            return Err(SYNTAX.into());
        }
        let prefix = map.get(&(env, account.to_owned())).ok_or_else(|| {
            format!(
                "no saved private registry list for {env} account '{account}' — run 'lastdb app show <app> --env {env} --save' or 'lastdb app publish' first"
            )
        })?;
        return Ok((
            env,
            format_locator(env, account),
            private_index_url(env, prefix),
        ));
    }
    let env = Environment::try_from_str(rest).map_err(|_| SYNTAX.to_owned())?;
    resolve_for_env(&map, env)
}

fn resolve_for_env(
    map: &BTreeMap<(Environment, String), String>,
    env: Environment,
) -> Result<(Environment, String, String), String> {
    let matches: Vec<(&String, &String)> = map
        .iter()
        .filter_map(|((e, account), prefix)| (*e == env).then_some((account, prefix)))
        .collect();
    match matches.as_slice() {
        [] => Err(format!(
            "no saved private registry list for {env} — run 'lastdb app show <app> --env {env} --save' or 'lastdb app publish' first"
        )),
        [(account, prefix)] => Ok((
            env,
            format_locator(env, account),
            private_index_url(env, prefix),
        )),
        many => {
            let choices: Vec<String> = many
                .iter()
                .map(|(account, _)| format_locator(env, account))
                .collect();
            Err(format!(
                "multiple saved {env} private registry lists ({}); pass --index with one locator",
                choices.join(", ")
            ))
        }
    }
}

pub(crate) fn saved_locator_for_account(
    env: Environment,
    account: &str,
) -> Result<Option<String>, String> {
    if !valid_owner_account(account) {
        return Ok(None);
    }
    let map = load_map()?;
    Ok(map
        .contains_key(&(env, account.to_owned()))
        .then(|| format_locator(env, account)))
}

pub(crate) fn saved_locators_for_env(
    env: Environment,
    preferred_account: Option<&str>,
) -> Result<Vec<String>, String> {
    let map = load_map()?;
    let mut locators = Vec::new();
    if let Some(pref) = preferred_account.filter(|a| valid_owner_account(a)) {
        if map.contains_key(&(env, pref.to_owned())) {
            locators.push(format_locator(env, pref));
        }
    }
    for (entry_env, account) in map.keys() {
        if *entry_env == env && preferred_account != Some(account.as_str()) {
            locators.push(format_locator(env, account));
        }
    }
    Ok(locators)
}
