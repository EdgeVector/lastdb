//! Reading the signed public index: list a channel, resolve one app, or
//! print one catalog entry. Install and upgrade go through `managed`.

// The handlers take the parsed CLI fields by value: the dispatcher destructures
// an owned `AppCommand`, so borrowing would only add clones at the call sites.
#![allow(clippy::needless_pass_by_value)]

use super::*;

pub(crate) fn app_list(
    home: &Path,
    channel: Option<String>,
    index: Option<String>,
    trust_key: Option<String>,
    json: bool,
) -> Result<(), String> {
    let list = public_list(
        home,
        channel.as_deref(),
        index.as_deref(),
        trust_key.as_deref(),
    )?;
    print_public_list(&list, json);
    Ok(())
}

pub(crate) fn app_resolve(home: &Path, query: &AppResolveQuery<'_>) -> Result<(), String> {
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let resolution = resolve_by_proof(
        &socket,
        query.app_id,
        query.channel,
        query.index,
        query.trust_key,
    )?;
    if query.json {
        if let Some(entry) = &resolution.list_entry {
            print_pretty_json(entry);
        } else {
            print_pretty_json(&resolution);
        }
    } else {
        println!(
            "{} {} @ {} proved with lastdb {} ({}) by {} at {}",
            resolution.app_id,
            resolution.app_version,
            resolution.sha,
            resolution.lastdb_version,
            resolution.lastdb_version_source,
            resolution.proof_run,
            resolution.proved_at
        );
        println!("  source: {}", resolution.source);
        println!(
            "  index: {} (trust: {})",
            resolution.index, resolution.trust
        );
    }
    Ok(())
}

pub(crate) fn app_info(
    home: &Path,
    app_id: &str,
    channel: Option<String>,
    index: Option<String>,
    trust_key: Option<String>,
) -> Result<(), String> {
    let list = public_list(
        home,
        channel.as_deref(),
        index.as_deref(),
        trust_key.as_deref(),
    )?;
    let entry = list
        .apps
        .iter()
        .find(|entry| entry.app_id == app_id)
        .ok_or_else(|| {
            format!(
                "no app '{app_id}' was proved with lastdb API {}",
                list.api_version
            )
        })?;
    print_pretty_json(entry);
    Ok(())
}
