//! Target selection follows complete independent physical source proof.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::{DateTime, Utc};
use fold_db::atom::atom_row_header;
use fold_db::db_operations::AtomStore;
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn build(
    args: &TargetAtomGcArgs,
    opened: &HomeStore,
    started_at: &str,
) -> Result<model::Plan, String> {
    let inputs = input::read(args)?;
    let cutoff = DateTime::parse_from_rfc3339(started_at)
        .map_err(err)?
        .with_timezone(&Utc);
    if cutoff > Utc::now() {
        return Err("the target atom cutoff is in the future".into());
    }
    let collected =
        crate::atom_source_proof::collect(&args.home, opened, &inputs.uuids, &args.plan_dir)
            .await?;
    if !collected.facts.complete || !collected.facts.cloud_gate.complete {
        return Err("target atom source proof is incomplete".into());
    }
    let mut grouped = BTreeMap::<String, Vec<_>>::new();
    let mut physical_keys = BTreeSet::new();
    for body in collected.bodies {
        if !inputs.uuids.contains(&body.uuid) {
            return Err("source proof returned an unrequested atom body".into());
        }
        if !physical_keys.insert((body.collection.clone(), body.key.clone())) {
            return Err("target physical atom key occurs in more than one handle".into());
        }
        grouped.entry(body.uuid.clone()).or_default().push(body);
    }
    let found = grouped.keys().cloned().collect::<BTreeSet<_>>();
    let absent = inputs
        .uuids
        .difference(&found)
        .cloned()
        .collect::<BTreeSet<_>>();
    if found != collected.facts.found_target_ids || absent != collected.facts.missing_target_ids {
        return Err("target body inventory differs from complete physical proof".into());
    }
    let (e2e, _) = lastdb_node::offline_home::load_e2e_keys(&args.home)?;
    let decoder = AtomStore::for_offline_read(
        Arc::clone(&opened.store),
        e2e.encryption_key(),
        e2e.encryption_key(),
    )
    .await
    .map_err(err)?;
    let mut counts = model::Counts {
        requested_uuids: inputs.uuids.len() as u64,
        found_uuids: found.len() as u64,
        target_copies_read: grouped.values().map(|bodies| bodies.len() as u64).sum(),
        absent_uuids: absent.len() as u64,
        ..Default::default()
    };
    let mut retained = BTreeMap::new();
    let mut eligible = Vec::new();
    for (uuid, mut bodies) in grouped {
        bodies.sort_by(|a, b| (&a.collection, &a.key).cmp(&(&b.collection, &b.key)));
        let reasons = hold_reasons(&uuid, &bodies, &collected.facts, cutoff)?;
        if !reasons.is_empty() {
            count_retained(&reasons, bodies.len() as u64, &mut counts);
            retained.insert(uuid, reasons);
            continue;
        }
        eligible.extend(bodies);
    }
    let candidates = decode_candidates(&eligible, &decoder).await?;
    for candidate in &candidates {
        counts.candidate_copies += candidate.copies.len() as u64;
        counts.candidate_raw_bytes += candidate
            .copies
            .iter()
            .map(|copy| copy.raw_bytes)
            .sum::<u64>();
        counts.derived_storage_keys += candidate.derived_keys.len() as u64;
    }
    counts.candidate_uuids = candidates.len() as u64;
    if input::read(args)? != inputs {
        return Err("the target inputs changed during source proof".into());
    }
    Ok(model::Plan {
        format: model::FORMAT,
        home: std::fs::canonicalize(&args.home).map_err(err)?,
        store_root: std::fs::canonicalize(&opened.store_root).map_err(err)?,
        started_at: started_at.into(), input: inputs, proof: collected.facts,
        counts, found_uuids: found, absent_uuids: absent, retained, candidates,
        retirement_state_sha256: model::retirement_state(&opened.store_root)?,
        prerequisites: vec![
            "required external gate: the prior GC handler returned; an interrupted manual ledger remains unconfirmed".into(),
            "required external gate: clean shutdown receipt and daemon process exit before this stopped window".into(),
            "required external gate: successful authoritative normal snapshot after key reap under the established local writer pause".into(),
            "scope: local orphan GC only; remote erasure is outside this proof".into(),
            "required external gate: restart the same daemon; use normal owner atom compaction and a new retirement snapshot before writers resume".into(),
        ],
    })
}

fn hold_reasons(
    uuid: &str,
    bodies: &[crate::atom_source_proof::TargetBody],
    facts: &crate::atom_source_proof::Facts,
    cutoff: DateTime<Utc>,
) -> Result<BTreeSet<String>, String> {
    let mut reasons = BTreeSet::new();
    if facts.holds_uuid(uuid) {
        reasons.insert("retained-source-or-edge".into());
    }
    for body in bodies {
        if body.collection != "atoms" || !body.scope.is_empty() {
            reasons.insert("other-physical-scope-or-collection".into());
        }
        let header = atom_row_header(&body.plain).map_err(err)?;
        if header.get("uuid").and_then(serde_json::Value::as_str) != Some(uuid) {
            return Err("target atom header differs from its physical identity".into());
        }
        match created_at(&header) {
            Some(time) if time < cutoff => {}
            Some(_) => {
                reasons.insert("recent-source".into());
            }
            None => {
                reasons.insert("undated-source".into());
            }
        }
    }
    Ok(reasons)
}

fn created_at(header: &serde_json::Value) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(header.get("created_at")?.as_str()?)
        .ok()
        .map(|time| time.with_timezone(&Utc))
}

fn count_retained(reasons: &BTreeSet<String>, copies: u64, counts: &mut model::Counts) {
    counts.retained_uuids += 1;
    counts.held_uuids += u64::from(reasons.contains("retained-source-or-edge"));
    counts.other_scope_uuids += u64::from(reasons.contains("other-physical-scope-or-collection"));
    counts.recent_uuids += u64::from(reasons.contains("recent-source"));
    counts.undated_uuids += u64::from(reasons.contains("undated-source"));
    counts.recent_copies += if reasons.contains("recent-source") {
        copies
    } else {
        0
    };
    counts.undated_copies += if reasons.contains("undated-source") {
        copies
    } else {
        0
    };
}

async fn decode_candidates(
    bodies: &[crate::atom_source_proof::TargetBody],
    decoder: &AtomStore,
) -> Result<Vec<model::Candidate>, String> {
    let mut by_uuid = BTreeMap::<String, (Vec<model::BodyCopy>, BTreeSet<String>)>::new();
    for batch in bodies.chunks(1000) {
        let plain = batch
            .iter()
            .map(|body| body.plain.clone())
            .collect::<Vec<_>>();
        let decoded = decoder
            .decode_stored_atom_batch(&plain)
            .await
            .map_err(err)?;
        if decoded.len() != batch.len() {
            return Err("target atom decoder batch count differs".into());
        }
        for (body, atom) in batch.iter().zip(decoded) {
            if atom.uuid() != body.uuid {
                return Err("decoded target atom differs from its physical identity".into());
            }
            let (copies, derived) = by_uuid.entry(body.uuid.clone()).or_default();
            derived.extend(AtomStore::offline_reclaim_derived_keys(&atom, None).map_err(err)?);
            copies.push(model::BodyCopy {
                collection: body.collection.clone(),
                shard: body.shard,
                group_id: body.group_id,
                key_b64: STANDARD.encode(&body.key),
                raw_sha256: model::digest(&body.raw),
                raw_bytes: body.raw.len() as u64,
                created_at: atom.created_at().to_rfc3339(),
            });
        }
    }
    let mut candidates = Vec::new();
    for (uuid, (copies, mut derived)) in by_uuid {
        let locator = fold_db::atom::atom_locator_codec::locator_key(&uuid);
        if !derived.remove(&locator) {
            return Err("the production delete keys omitted the atom locator".into());
        }
        let mut derived_keys = derived.into_iter().collect::<Vec<_>>();
        derived_keys.push(locator);
        candidates.push(model::Candidate {
            uuid,
            copies,
            derived_keys,
        });
    }
    Ok(candidates)
}
