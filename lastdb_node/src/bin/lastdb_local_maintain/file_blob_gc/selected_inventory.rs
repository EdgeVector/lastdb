//! Selected local body presence, independent of age and reference holds.
//!
//! This read-only report grants no delete authority and proves no remote
//! absence. Exact identities stay in a private manifest and streamed JSONL.

use super::*;
use std::collections::{BTreeMap, BTreeSet};

mod model;
mod reader;
mod source;

pub(super) async fn run(args: &FileBlobGcArgs, opened: &HomeStore) -> Result<(), String> {
    let input = model::read_input(args)?;
    let home = std::fs::canonicalize(&args.home).map_err(err)?;
    let store_root = std::fs::canonicalize(&opened.store_root).map_err(err)?;
    super::model::create_plan_dir(&args.plan_dir, &home, &store_root)?;
    let retirement = super::model::retirement_state(&store_root)?;
    let physical = opened
        .base
        .raw_last_store()
        .ok_or("home has no physical LastStore")?;
    let crypto =
        crate::home::load_home_crypto(&args.home).ok_or("home has no at-rest identity key")?;
    let names = namespace_names(opened).await?;
    let mut state = model::Collected::new(&args.plan_dir)?;
    for name in &names {
        reader::collection(name, &physical, &crypto, &input.refs, &mut state).await?;
    }
    if namespace_names(opened).await? != names {
        return Err("physical namespaces changed during selected blob inventory".into());
    }
    if model::read_input(args)? != input {
        return Err("selected blob input changed during inventory".into());
    }
    prove_stopped(args)?;
    if super::model::retirement_state(&store_root)? != retirement {
        return Err("atom retirement state changed during selected blob inventory".into());
    }
    let copies = state.copies.finish()?;
    let absent = input
        .refs
        .difference(&state.found)
        .cloned()
        .collect::<BTreeSet<_>>();
    let report = model::Inventory {
        format: 1, home, store_root, created_at: chrono::Utc::now().to_rfc3339(),
        input_file_sha256: input.sha256, requested_refs: input.refs,
        found_refs: state.found, absent_refs: absent, namespace_digests: state.digests,
        counts: state.counts, copies, retirement_state_sha256: retirement,
        complete: true, read_only: true, delete_authority: false,
        reference_holds_checked: false, remote_absence_proved: false,
        scope_note: "all supported live physical local file-blob copies, before age or reference filters; this does not prove raw obsolete disk bytes or remote erasure",
    };
    super::model::write_private(&args.plan_dir, "selected-file-blob-inventory.json", &report)?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string(&model::PublicReport::from(&report)).map_err(err)?
        );
    } else {
        println!("selected file blob inventory complete; use --json for numeric counts.");
    }
    Ok(())
}

async fn namespace_names(opened: &HomeStore) -> Result<Vec<String>, String> {
    let mut names = opened.base.list_namespaces().await.map_err(err)?;
    names.sort();
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err("duplicate physical namespace".into());
    }
    Ok(names)
}
