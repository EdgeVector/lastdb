//! Exercise explicit cleanup inside the existing CI-only run-home operation.
//! This checks physical compaction, not a blob counter or node completion claim.

use crate::gen::{apply, Model, Rng};
use crate::{budget, dir_bytes, run_files, Result};
use laststore::disk_run::home::{HomeMutation, RunHome, RunRead, MIN_BASE_MERGE_BYTES};
use std::path::Path;

pub fn run(scratch: &Path, key: &[u8; 32]) -> Result<String> {
    let small = small_delete(scratch, key)?;
    let resumed = reopen_progress(scratch, key)?;
    Ok(format!("{small}; {resumed}"))
}

fn seed(dir: &Path, key: &[u8; 32], count: usize, bytes: usize) -> Result<(RunHome, Model)> {
    let home = RunHome::create(dir, key)?;
    let mut rng = Rng::new(0xc1ea_2026);
    let operations: Vec<_> = (0..count)
        .map(|n| {
            let mut body = Vec::with_capacity(bytes);
            while body.len() < bytes {
                body.extend_from_slice(&rng.next_u64().to_le_bytes());
            }
            body.truncate(bytes);
            HomeMutation {
                collection: "blob_counts".into(),
                key: format!("cleanup:{n:08}"),
                body: Some(body),
            }
        })
        .collect();
    let mut model = Model::new();
    apply(&mut model, &operations);
    home.commit(operations)?;
    Ok((home, model))
}

fn delete(home: &RunHome, model: &mut Model, count: usize) -> Result<()> {
    let operations: Vec<_> = (0..count)
        .map(|n| HomeMutation {
            collection: "blob_counts".into(),
            key: format!("cleanup:{n:08}"),
            body: None,
        })
        .collect();
    apply(model, &operations);
    home.commit(operations)?;
    Ok(())
}

fn equal(read: &mut RunRead, model: &Model) -> Result<()> {
    let keys: Vec<_> = model.keys().cloned().collect();
    let found = read.get_many(&keys)?;
    if found.len() != model.len() {
        return Err("cleanup batch result count differs".into());
    }
    for ((coordinate, expected), got) in model.iter().zip(found) {
        if got? != *expected {
            return Err(format!("cleanup read differs at {coordinate:?}").into());
        }
    }
    Ok(())
}

fn drain(home: &RunHome) -> Result<usize> {
    for steps in 0..512 {
        if home.cleanup_step()?.is_none() {
            return Ok(steps);
        }
    }
    Err("cleanup did not settle inside the operation budget".into())
}

fn small_delete(scratch: &Path, key: &[u8; 32]) -> Result<String> {
    let dir = scratch.join("cleanup-small");
    // Keep this existing explicit-cleanup operation below the 64 KiB stale
    // floor. Larger deleted bodies now compact through the automatic path.
    let (home, mut model) = seed(&dir, key, 512, 32)?;
    let before = model.clone();
    let mut held = home.read(budget());
    delete(&home, &mut model, 256)?;
    while home.merge_step()?.is_some() {}
    let status = home.status();
    if home.needs_merge()
        || status
            .stale_bytes
            .map_or(true, |n| n == 0 || n >= MIN_BASE_MERGE_BYTES)
        || status.levels.is_empty()
    {
        return Err("small delete did not remain below the automatic threshold".into());
    }
    equal(&mut home.read(budget()), &model)?;
    let old_disk = dir_bytes(&dir)?;
    let frontier = status.frontier;
    let steps = drain(&home)?;
    let clean = home.status();
    if steps == 0 || clean.frontier != frontier || clean.records() != 256 {
        return Err("explicit cleanup changed commits or kept dead records".into());
    }
    equal(&mut home.read(budget()), &model)?;
    equal(&mut held, &before)?;
    let with_held = run_files(&dir)?.0;
    if with_held <= clean.runs() || home.retired_bytes() == 0 {
        return Err("cleanup did not retain the old read files".into());
    }
    drop(held);
    let (after, temporary) = run_files(&dir)?;
    let new_disk = dir_bytes(&dir)?;
    if after != clean.runs() || temporary != 0 || home.retired_bytes() != 0 || new_disk >= old_disk
    {
        return Err("cleanup did not release obsolete files after the read".into());
    }
    let generation = clean.generation;
    if home.cleanup_step()?.is_some() || home.status().generation != generation {
        return Err("repeat cleanup was not idle".into());
    }
    drop(home);
    let reopened = RunHome::open(&dir, key)?;
    equal(&mut reopened.read(budget()), &model)?;
    if reopened.cleanup_step()?.is_some() {
        return Err("completed cleanup returned after reopen".into());
    }
    Ok(format!(
        "small delete: 512 seeded, 256 deleted, below-threshold stale {} B, {steps} base steps, frontier {frontier} unchanged, old read 512 equal, files {with_held}->{after}, disk {old_disk}->{new_disk} B, repeat/reopen idle",
        status.stale_bytes.unwrap_or(0)
    ))
}

fn reopen_progress(scratch: &Path, key: &[u8; 32]) -> Result<String> {
    let dir = scratch.join("cleanup-resume");
    let (home, mut model) = seed(&dir, key, 4096, 512)?;
    drain(&home)?;
    if home.status().base.0 < 2 {
        return Err("cleanup resume needs multiple base files".into());
    }
    delete(&home, &mut model, 128)?;
    while home.merge_step()?.is_some() {}
    if home.cleanup_step()?.is_none() || home.status().merging.0 == 0 {
        return Err("cleanup resume did not publish an unfinished base merge".into());
    }
    let frontier = home.status().frontier;
    let next = HomeMutation {
        collection: "blob_counts".into(),
        key: "cleanup:00000000".into(),
        body: Some(b"new value after the cleanup step".to_vec()),
    };
    apply(&mut model, std::slice::from_ref(&next));
    home.commit(vec![next])?;
    drop(home);
    let reopened = RunHome::open(&dir, key)?;
    if reopened.status().merging.0 == 0 || reopened.status().frontier != frontier + 1 {
        return Err("cleanup progress or the later commit did not survive reopen".into());
    }
    equal(&mut reopened.read(budget()), &model)?;
    let steps = drain(&reopened)?;
    equal(&mut reopened.read(budget()), &model)?;
    let live = model.values().filter(|v| v.is_some()).count();
    let status = reopened.status();
    if status.records() != live as u64
        || status.frontier != frontier + 1
        || run_files(&dir)?.0 != status.runs()
    {
        return Err("resumed cleanup kept dead records or lost a later commit".into());
    }
    Ok(format!(
        "resume: 4096 seeded, 128 deleted, published partial merge reopened, later write retained, {steps} remaining steps, {live} live records equal, frontier {} unchanged by cleanup",
        status.frontier
    ))
}
