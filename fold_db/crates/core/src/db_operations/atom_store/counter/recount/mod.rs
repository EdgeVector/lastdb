//! The recount: rebuild every counter from the tips and the conflict records.
//!
//! `recount_counters` is the one scan the counter design keeps. It reads every
//! tip record and every counted conflict record of a scope, counts the
//! references of each atom, and compares the count with the counter record of
//! the atom. It writes the exact count where the counter differs, creates a
//! missing counter, deletes a counter that has no body, and deletes a body that
//! no tip and no record names. After it, I1 and I2 hold.
//!
//! **Offline only.** It takes no write gate and has no atomic scope with the
//! planner. The caller opens a stopped home (`lastdb db recount` refuses a home
//! that a daemon serves). No gate or write path calls it. It is safe to run
//! again: a run that stopped leaves every batch whole, and the next run changes
//! only what is still wrong.
//!
//! Memory: one pass holds a count for each referenced atom of its shard. A
//! second shard count (`shards`) makes several passes over the tips and records
//! and holds `1/shards` of the atoms in each.
//!
//! The recount refuses to write while any tip or conflict row does not decode:
//! it could not count the references of such a row, and a count that is too low
//! loses data.

mod apply;
mod blobs;
mod decide;
mod report;
mod retention;

pub use report::{Change, RecountOptions, RecountReport};

use super::scan::{family_range, record_holders, tip_holders, Family};
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;
use apply::Pending;
use decide::decide;
use std::collections::HashMap;
use std::time::Instant;

/// Rows asked for in each read of the tips and records.
const TALLY_PAGE_ROWS: usize = 2_000;
/// Rows asked for in each read of the atoms.
const ATOM_PAGE_ROWS: usize = 1_000;

/// The refusal of a recount that met rows without an at-rest envelope.
fn unsealed_refusal(reads: u64) -> SchemaError {
    SchemaError::InvalidData(format!(
        "{reads} read(s) dropped a row with no at-rest envelope; such a row reads as absent, so the recount cannot count it and writes nothing. \
         Run `lastdb db reap-unsealed --collection <name>` for the collections that hold them (dry run first), then recount again"
    ))
}

/// One pass: a scope and a shard of the atom ids.
struct Pass<'a> {
    scope: Option<&'a str>,
    index: u32,
    count: u32,
}

impl Pass<'_> {
    fn owns(&self, atom: &str) -> bool {
        let hash = atom.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        });
        hash % u64::from(self.count) == u64::from(self.index)
    }
}

impl AtomStore {
    /// Rebuild the counters of the given scopes from the tips and records.
    /// Offline only. See the module documentation.
    pub async fn recount_counters(
        &self,
        options: &RecountOptions,
    ) -> Result<RecountReport, SchemaError> {
        let started = Instant::now();
        let shards = options.shards.max(1);
        let mut report = RecountReport {
            executed: options.execute,
            scopes: options.scopes.clone(),
            shards,
            ..RecountReport::default()
        };
        let before = self.range_walks().snapshot();
        for scope in &options.scopes {
            for index in 0..shards {
                let pass = Pass {
                    scope: scope.as_deref(),
                    index,
                    count: shards,
                };
                self.recount_pass(&pass, options.execute, &mut report)
                    .await?;
            }
        }
        self.recount_blobs(options, &mut report).await?;
        report.range_reads = self.range_walks().snapshot().since(before);
        report.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok(report)
    }

    async fn recount_pass(
        &self,
        pass: &Pass<'_>,
        execute: bool,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let mut tally: HashMap<String, u32> = HashMap::new();
        self.tally_family(Family::Tips, pass, &mut tally, report)
            .await?;
        self.tally_family(Family::Records, pass, &mut tally, report)
            .await?;
        if report.unreadable_rows > 0 && execute {
            return Err(SchemaError::InvalidData(format!(
                "{} tip or conflict rows do not decode; the recount cannot count their references and writes nothing",
                report.unreadable_rows
            )));
        }
        if report.unsealed_row_reads > 0 && execute {
            return Err(unsealed_refusal(report.unsealed_row_reads));
        }
        report.referenced_atoms += tally.len() as u64;
        self.recount_atoms(pass, &mut tally, execute, report)
            .await?;
        // An atom that tips or records name and that has neither a body nor a
        // counter never came up in the walk of the atoms.
        report.lost_bodies += tally.len() as u64;
        // The retention rows of the records do not depend on the shard: one pass
        // over the records of the scope checks them.
        if pass.index == 0 {
            self.recount_retention(pass.scope, execute, report).await?;
        }
        Ok(())
    }

    /// Count the holders of each atom of the pass from the tips or the records.
    async fn tally_family(
        &self,
        family: Family,
        pass: &Pass<'_>,
        tally: &mut HashMap<String, u32>,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let (start, end) = family_range(family, pass.scope);
        let mut walk = RangeWalk::new(&start, &end, TALLY_PAGE_ROWS);
        let first_pass = pass.index == 0;
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            let rows = step.rows;
            let (holders, bad) = match family {
                Family::Records => record_holders(&rows),
                _ => tip_holders(&rows),
            };
            if first_pass {
                match family {
                    Family::Records => report.records_read += rows.len() as u64,
                    _ => report.tips_read += rows.len() as u64,
                }
                report.unreadable_rows += bad.total;
                report.unsealed_row_reads += step.unsealed;
            }
            for (atom, count) in holders {
                if pass.owns(&atom) {
                    *tally.entry(atom).or_insert(0) += count;
                }
            }
        }
        Ok(())
    }

    /// Walk the bodies and counters of the scope, decide for each atom of the
    /// pass, and write in batches.
    async fn recount_atoms(
        &self,
        pass: &Pass<'_>,
        tally: &mut HashMap<String, u32>,
        execute: bool,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let (start, end) = family_range(Family::Atoms, pass.scope);
        let mut walk = RangeWalk::new(&start, &end, ATOM_PAGE_ROWS);
        let mut pending = Pending::new(pass.scope);
        while let Some((read_start, _, _)) = walk.request() {
            let step = self.walk_step(&mut walk).await?;
            let paired = self.pair_rows(&step.rows, &read_start, pass.scope).await?;
            // A row without an at-rest envelope reads as absent. A body or a
            // counter that is only absent for that reason must not decide a
            // delete, so an execute stops here and writes nothing of this page.
            let dropped = step.unsealed + paired.unsealed;
            if pass.index == 0 {
                report.unsealed_row_reads += dropped;
            }
            if dropped > 0 && execute {
                return Err(unsealed_refusal(dropped));
            }
            let may_delete = report.unreadable_rows == 0 && report.unsealed_row_reads == 0;
            for pair in paired
                .pairs
                .into_iter()
                .filter(|pair| pass.owns(&pair.atom))
            {
                let holders = tally.remove(&pair.atom).map_or(0, u64::from);
                report.counters_read +=
                    u64::from(!matches!(pair.counter, super::scan::CounterRead::Absent));
                report.bodies_read += u64::from(pair.body);
                let decision = decide(&pair, holders, may_delete);
                self.plan_decision(&mut pending, &pair, decision, report)
                    .await?;
                if pending.is_full() {
                    self.flush_pending(&mut pending, execute, report).await?;
                }
            }
        }
        self.flush_pending(&mut pending, execute, report).await
    }
}
