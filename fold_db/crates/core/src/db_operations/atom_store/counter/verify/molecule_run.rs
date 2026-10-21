//! The molecule phases of a verify run: headers, `bindings` counters, and
//! binding records. The verifier reads and reports. It never changes a counter.

use super::molecule_judge::{judge_bindings, judge_header, HeaderSeen};
use super::molecule_pages::{BindingRecordsPage, BindingsPage, HeadersPage};
use super::report::{Kind, Phase};
use super::state::{Run, StepGuard};
use super::totals::{digest, Watch};
use crate::db_operations::atom_store::{counts_of_header, AtomStore};
use crate::schema::SchemaError;

/// What one page of a molecule phase gave.
enum MoleculePage {
    Headers(HeadersPage),
    Bindings(BindingsPage),
    Records(BindingRecordsPage),
}

impl MoleculePage {
    /// The rows read, the rows the scan dropped, the point reads that dropped a
    /// row, and the last key.
    fn shape(&self) -> (usize, u64, u64, Option<Vec<u8>>) {
        match self {
            Self::Headers(page) => (page.rows, page.scan_unsealed, 0, page.resume.clone()),
            Self::Bindings(page) => (
                page.rows,
                page.scan_unsealed,
                page.point_unsealed,
                page.resume.clone(),
            ),
            Self::Records(page) => (
                page.rows,
                page.scan_unsealed,
                page.point_unsealed,
                page.resume.clone(),
            ),
        }
    }
}

impl AtomStore {
    /// Read and judge one page of a molecule phase.
    pub(super) async fn verify_molecule_page(
        &self,
        guard: &mut StepGuard<'_>,
    ) -> Result<(), SchemaError> {
        let (run, watch) = guard.split();
        let request = run
            .walk
            .request()
            .expect("a walk that is not done has a read");
        let scope = run.report.scope.clone();
        let read = match run.phase {
            Phase::Headers => self
                .read_headers_page(request)
                .await
                .map(MoleculePage::Headers),
            Phase::Bindings => self
                .read_bindings_page(scope.as_deref(), request)
                .await
                .map(MoleculePage::Bindings),
            _ => self
                .read_binding_records_page(scope.as_deref(), request)
                .await
                .map(MoleculePage::Records),
        };
        match read {
            Ok(page) => {
                let (rows, scan_unsealed, point_unsealed, resume) = page.shape();
                run.note_unsealed(scan_unsealed + point_unsealed);
                run.walk
                    .finish_yield(rows, scan_unsealed, resume.as_deref());
                apply_page(run, watch, page);
            }
            Err(SchemaError::ReadResourceLimit { .. }) => {
                run.report.page_retries += 1;
                if !run.walk.read_too_big() {
                    return Err(SchemaError::ReadResourceLimit {
                        detail: "a key range is too dense to read in pieces".into(),
                    });
                }
            }
            Err(error) => return Err(error),
        }
        let stats = run.walk.take_stats();
        run.report.range_reads.absorb(stats);
        self.range_walks().add(stats);
        Ok(())
    }
}

fn apply_page(run: &mut Run, watch: &Watch, page: MoleculePage) {
    run.report.pages += 1;
    match page {
        MoleculePage::Headers(page) => apply_headers(run, watch, page),
        MoleculePage::Bindings(page) => apply_bindings(run, page),
        MoleculePage::Records(page) => apply_binding_records(run, page),
    }
}

/// Judge the headers of a page against the totals of the first two phases.
fn apply_headers(run: &mut Run, watch: &Watch, page: HeadersPage) {
    for row in page.headers {
        run.report.headers_read += 1;
        let key = digest(&row.molecule);
        run.headers_seen.insert(key);
        let counts = row.value.as_ref().and_then(counts_of_header);
        let seen = HeaderSeen {
            tips: run.molecule_tips.holders(key),
            records: run.molecule_records.holders(key),
            touched: watch.touched(key),
            complete: run.molecule_tips.complete && run.molecule_records.complete,
        };
        for found in judge_header(counts.as_ref(), row.value.is_some(), &seen) {
            run.note_fault(found.kind, &row.molecule, found.detail);
        }
    }
}

/// Judge each `bindings` counter against its binding records, read at one root.
fn apply_bindings(run: &mut Run, page: BindingsPage) {
    for row in page.counters {
        run.report.binding_counters_read += 1;
        run.report.binding_records_read += row.binding_records;
        for found in judge_bindings(row.record.as_ref(), row.binding_records) {
            run.note_fault(found.kind, &row.molecule_token, found.detail);
        }
    }
}

/// Report each binding record whose molecule has no `bindings` counter.
fn apply_binding_records(run: &mut Run, page: BindingRecordsPage) {
    for token in page.without_counter {
        run.note_fault(
            Kind::BindingRecordWithoutCounter,
            &token,
            "binding records exist and the bindings counter does not".into(),
        );
    }
}

/// End of the headers phase: a molecule that has tips and no header.
pub(super) fn finish_headers(run: &mut Run) {
    if !run.molecule_tips.complete {
        return;
    }
    let missing: Vec<(u128, u32)> = run
        .molecule_tips
        .entries()
        .filter(|(key, tips)| *tips > 0 && !run.headers_seen.contains(key))
        .collect();
    for (key, tips) in missing {
        run.note_fault(
            Kind::TipsWithoutHeader,
            &format!("digest:{key:032x}"),
            format!("{tips} tips name a molecule that has no header"),
        );
    }
}
