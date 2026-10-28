//! Real merge admission and retry inside the existing CI-only home operation.

use crate::gen::{apply, Model, Rng};
use crate::{budget, check_all, dir_bytes, Result};
use laststore::disk_run::home::{HomeMutation, RangeBounds, RunHome, RunMerge};
use laststore::space::{available_space, reserve_space, SpacePolicy};
use std::fs;
use std::path::Path;

const MIB: u64 = 1024 * 1024;
const PREFIX: &str = "space\0";

#[derive(Clone, Copy)]
enum Operation {
    Tiered,
    Base,
    Cleanup,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Tiered => "tiered",
            Self::Base => "automatic-base",
            Self::Cleanup => "explicit-cleanup",
        }
    }

    fn step(self, home: &RunHome, policy: &SpacePolicy) -> laststore::Result<Option<RunMerge>> {
        match self {
            Self::Tiered | Self::Base => home.merge_step_with_space(policy),
            Self::Cleanup => home.cleanup_step_with_space(policy),
        }
    }
}

pub fn run(scratch: &Path, key: &[u8; 32]) -> Result<String> {
    let mut results = Vec::new();
    for operation in [Operation::Tiered, Operation::Base, Operation::Cleanup] {
        results.push(one_operation(scratch, key, operation)?);
    }
    results.push(near_cap_reclaim(scratch, key)?);
    Ok(results.join("; "))
}

fn seed(dir: &Path, key: &[u8; 32], operation: Operation) -> Result<(RunHome, Model)> {
    let home = RunHome::create(dir, key)?;
    let (batches, count, bytes) = match operation {
        Operation::Tiered => (8, 512, 512),
        Operation::Base => (1, 4096, 512),
        Operation::Cleanup => (1, 128, 32),
    };
    let mut rng = Rng::new(0x2026_c0a0_5ace);
    let mut model = Model::new();
    for batch in 0..batches {
        let operations: Vec<_> = (0..count)
            .map(|index| {
                let number = batch * count + index;
                let mut body = Vec::with_capacity(bytes);
                while body.len() < bytes {
                    body.extend_from_slice(&rng.next_u64().to_le_bytes());
                }
                body.truncate(bytes);
                HomeMutation {
                    collection: "tips".into(),
                    key: format!("{PREFIX}{number:08}"),
                    body: Some(body),
                }
            })
            .collect();
        apply(&mut model, &operations);
        home.commit(operations)?;
    }
    Ok((home, model))
}

fn one_operation(scratch: &Path, key: &[u8; 32], operation: Operation) -> Result<String> {
    let dir = scratch.join(format!("compaction-space-{}", operation.name()));
    let (home, model) = seed(&dir, key, operation)?;
    let initial = home.status();
    let policy = SpacePolicy::default();
    let bytes = dir_bytes(&dir)?;
    let mut limited = policy.clone();
    limited.disk_cap_bytes = bytes;
    refuse(&home, operation, &limited, &model)?;
    let partial = !matches!(operation, Operation::Cleanup);
    if partial {
        // One bounded output reservation fits, but the complete random data
        // cannot. This exercises removal of partially written merge outputs.
        limited.disk_cap_bytes += MIB + MIB / 4;
        if initial.file_bytes() <= MIB + MIB / 4 {
            return Err("compaction space operation needs more than one output reservation".into());
        }
        refuse(&home, operation, &limited, &model)?;
    }
    drop(home);
    let home = RunHome::open(&dir, key)?;
    equal(&home, &model)?;
    let mut steps = if matches!(operation, Operation::Cleanup) {
        0
    } else {
        shared_filesystem_reservation(scratch, &home, operation, &model)?
    };
    while operation.step(&home, &policy)?.is_some() {
        steps += 1;
        if steps > 512 {
            return Err("compaction space retry exceeded its step budget".into());
        }
    }
    let complete = home.status();
    if steps == 0
        || complete.frontier != initial.frontier
        || complete.generation <= initial.generation
    {
        return Err("compaction space retry did not preserve its commit frontier".into());
    }
    equal(&home, &model)?;
    drop(home);
    let reopened = RunHome::open(&dir, key)?;
    let records = equal(&reopened, &model)?;
    if reopened.status().frontier != initial.frontier {
        return Err("compaction space retry frontier differs after reopen".into());
    }
    Ok(format!(
        "{}: admission{} refused without a disk-root change; {steps} retry steps, {records} keys and one paged range equal after refusal/reopen/retry, frontier {} unchanged",
        operation.name(),
        if partial { " and partial output" } else { "" },
        initial.frontier
    ))
}

fn refuse(home: &RunHome, operation: Operation, policy: &SpacePolicy, model: &Model) -> Result<()> {
    let before = files(home.path())?;
    let current = fs::read(home.path().join("CURRENT"))?;
    let status = home.status();
    match operation.step(home, policy) {
        Err(laststore::Error::Io(error))
            if error.raw_os_error() == Some(rustix::io::Errno::NOSPC.raw_os_error()) => {}
        Err(error) => return Err(format!("compaction space refusal returned {error}").into()),
        Ok(_) => return Err("compaction accepted a replacement without enough space".into()),
    }
    let after = home.status();
    if home.is_poisoned()
        || after.generation != status.generation
        || after.frontier != status.frontier
        || fs::read(home.path().join("CURRENT"))? != current
        || files(home.path())? != before
    {
        return Err("compaction space refusal changed CURRENT or left output files".into());
    }
    equal(home, model)?;
    Ok(())
}

fn equal(home: &RunHome, model: &Model) -> Result<usize> {
    let checked = check_all(home, model)?;
    let expected: Vec<_> = model
        .iter()
        .filter_map(|((_, key), value)| value.clone().map(|value| (key.clone(), value)))
        .collect();
    let mut read = home.read(budget());
    let mut range = read.hash_range(
        "tips",
        PREFIX,
        RangeBounds {
            start: String::new(),
            end: None,
        },
    )?;
    let mut found = Vec::new();
    loop {
        let page = range.next_page(97)?;
        found.extend(page.records);
        if page.exhausted {
            break;
        }
    }
    if found != expected {
        return Err("compaction space range differs from the committed values".into());
    }
    Ok(checked)
}

fn files(root: &Path) -> Result<Vec<(String, u64)>> {
    let mut found = Vec::new();
    for relative in ["", "runs"] {
        for entry in fs::read_dir(root.join(relative))? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_file() {
                found.push((
                    format!("{relative}/{}", entry.file_name().to_string_lossy()),
                    meta.len(),
                ));
            }
        }
    }
    found.sort_unstable();
    Ok(found)
}

fn shared_filesystem_reservation(
    scratch: &Path,
    home: &RunHome,
    operation: Operation,
    model: &Model,
) -> Result<usize> {
    let other = scratch.join(format!("space-ticket-{}", operation.name()));
    fs::create_dir(&other)?;
    let available = available_space(home.path())?;
    if available < 256 * MIB {
        return Err("compaction space operation needs 256 MiB of available CI disk".into());
    }
    let foreground = SpacePolicy {
        free_space_reserve_bytes: 0,
        replacement_reserve_bytes: 0,
        ..SpacePolicy::default()
    };
    let ticket = reserve_space(&other, &foreground, 64 * MIB)?;
    let constrained = SpacePolicy {
        free_space_reserve_bytes: available - 32 * MIB,
        replacement_reserve_bytes: 0,
        ..SpacePolicy::default()
    };
    // This sibling has the same filesystem and a separate reservation root.
    // Its admitted bytes must protect foreground capacity across both roots.
    refuse(home, operation, &constrained, model)?;
    drop(ticket);
    if operation.step(home, &constrained)?.is_none() {
        return Err("released filesystem reservation did not admit the pending merge".into());
    }
    equal(home, model)?;
    fs::remove_dir(&other)?;
    Ok(1)
}

fn near_cap_reclaim(scratch: &Path, key: &[u8; 32]) -> Result<String> {
    let dir = scratch.join("compaction-space-near-cap");
    let (home, mut model) = seed(&dir, key, Operation::Base)?;
    let deletes: Vec<_> = (0..2048)
        .map(|number| HomeMutation {
            collection: "tips".into(),
            key: format!("{PREFIX}{number:08}"),
            body: None,
        })
        .collect();
    apply(&mut model, &deletes);
    home.commit(deletes)?;
    let before = dir_bytes(&dir)?;
    let initial = home.status();
    let mut policy = SpacePolicy::default();
    policy.disk_cap_bytes = before + policy.replacement_reserve_bytes;
    match reserve_space(home.path(), &policy, 1) {
        Err(laststore::Error::Io(error))
            if error.raw_os_error() == Some(rustix::io::Errno::NOSPC.raw_os_error()) => {}
        Err(error) => return Err(format!("near-cap foreground refusal returned {error}").into()),
        Ok(_) => return Err("near-cap foreground write consumed replacement headroom".into()),
    }
    let mut steps = 0;
    while home.merge_step_with_space(&policy)?.is_some() {
        steps += 1;
        if steps > 512 {
            return Err("near-cap reclaim exceeded its step budget".into());
        }
    }
    let after = dir_bytes(&dir)?;
    let complete = home.status();
    if steps == 0
        || after >= before
        || after > policy.disk_cap_bytes
        || complete.frontier != initial.frontier
        || complete.generation <= initial.generation
    {
        return Err("near-cap compaction did not reclaim within replacement headroom".into());
    }
    equal(&home, &model)?;
    // The reclaimed bytes admit foreground work under the original policy.
    drop(reserve_space(home.path(), &policy, 1)?);
    drop(home);
    let reopened = RunHome::open(&dir, key)?;
    let records = equal(&reopened, &model)?;
    if reopened.status().frontier != initial.frontier {
        return Err("near-cap reclaim frontier differs after reopen".into());
    }
    Ok(format!(
        "near-cap reclaim: foreground refused, {steps} merge steps spent replacement headroom, disk {before}->{after} B, foreground admitted afterward, {records} keys and one paged range equal after reopen, frontier {} unchanged",
        initial.frontier
    ))
}
