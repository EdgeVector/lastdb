//! Complete read-only roots for a fixed, private target atom set.

mod auxiliary;
mod cloud;
mod diagnostics;
mod model;
mod ownership;
mod reader;
mod sources;

use crate::home::HomeStore;
pub(crate) use model::{Collected, Facts, TargetBody};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

pub(crate) async fn collect(
    home: &Path,
    opened: &HomeStore,
    targets: &BTreeSet<String>,
    diagnostic_dir: &Path,
) -> Result<Collected, String> {
    let diagnostics = diagnostics::Sink::new(diagnostic_dir, home, &opened.store_root)?;
    let gate = crate::reap::cloud_gate::check(home, opened)
        .await
        .map_err(err)?;
    let published = gate
        .published_maps
        .get("personal")
        .ok_or("atom proof has no personal writer map")?;
    let snapshot = cloud::snapshot(opened, published).await?;
    let names = reader::namespace_names(opened).await?;
    let mut collected = Collected::new(gate, snapshot, diagnostics);
    let mut links = sources::Links::default();
    ownership::schemas(home, opened, &mut collected.facts).await?;
    let physical = opened
        .base
        .raw_last_store()
        .ok_or("atom proof has no physical LastStore")?;
    let crypto =
        crate::home::load_home_crypto(home).ok_or("atom proof has no at-rest identity key")?;
    for name in &names {
        reader::collection(
            name,
            &physical,
            &crypto,
            targets,
            &mut links,
            &mut collected,
        )
        .await?;
    }
    cloud::roots(opened, targets, &mut links, &mut collected.facts).await?;
    links.finish(targets, &mut collected.facts)?;
    reader::completion(opened, &collected.facts).await?;
    if reader::namespace_names(opened).await? != names {
        return Err("physical namespace inventory changed during atom proof".into());
    }
    ownership::join(&mut collected.facts);
    ownership::explanation(&mut collected.facts);
    collected.facts.missing_target_ids = targets
        .difference(&collected.facts.found_target_ids)
        .cloned()
        .collect();
    collected.facts.complete = true;
    Ok(collected)
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn digest(bytes: &[u8]) -> String {
    fold_db::hex::hex_lower(Sha256::digest(bytes))
}
