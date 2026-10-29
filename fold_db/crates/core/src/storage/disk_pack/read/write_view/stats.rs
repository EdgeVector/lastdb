//! Fixed-name diagnostics for the common view; no keys or values enter logs.

use super::*;

#[derive(Default, serde::Serialize)]
pub(in super::super) struct WriteViewCounts {
    reused_records: u64,
    reused_bytes: u64,
    overlay_records: u64,
    tips_and_controls: u64,
    counters: u64,
    metadata: u64,
    atoms: u64,
    absences: u64,
    offered_records: u64,
    offered_bytes: u64,
}

pub(in super::super) struct WriteView {
    counts: Mutex<WriteViewCounts>,
    started_us: u64,
}

impl Default for WriteView {
    fn default() -> Self {
        Self {
            counts: Mutex::new(WriteViewCounts::default()),
            started_us: laststore::commit_timing::unix_us(),
        }
    }
}

impl WriteViewCounts {
    pub(in super::super) fn note(&mut self, selected: &WriteRecord) {
        if !laststore::commit_timing::enabled() {
            return;
        }
        if !selected.is_core() {
            self.overlay_records += 1;
            return;
        }
        let record = selected.record();
        self.reused_records += 1;
        self.reused_bytes += record.body.as_ref().map_or(0, Vec::len) as u64;
        self.absences += u64::from(record.body.is_none());
        match record.collection.as_str() {
            "tips" => self.tips_and_controls += 1,
            "atoms" if crate::atom::atom_key_codec::is_counter_key(&record.key) => {
                self.counters += 1;
            }
            "atoms" => self.atoms += 1,
            _ => self.metadata += 1,
        }
    }
}

impl ReadContext {
    pub(in super::super) fn note_write_view(&self, counts: &WriteViewCounts) -> StorageResult<()> {
        let Some(view) = &self.write_view else {
            return Ok(());
        };
        if !laststore::commit_timing::enabled() {
            return Ok(());
        }
        let mut total = view.counts.lock().map_err(|_| unavailable())?;
        total.reused_records += counts.reused_records;
        total.reused_bytes += counts.reused_bytes;
        total.overlay_records += counts.overlay_records;
        total.tips_and_controls += counts.tips_and_controls;
        total.counters += counts.counters;
        total.metadata += counts.metadata;
        total.atoms += counts.atoms;
        total.absences += counts.absences;
        Ok(())
    }

    pub(in super::super) fn note_write_view_offers(
        &self,
        observations: &[Observation<'_>],
    ) -> StorageResult<()> {
        let Some(view) = &self.write_view else {
            return Ok(());
        };
        if laststore::commit_timing::enabled() {
            let mut total = view.counts.lock().map_err(|_| unavailable())?;
            total.offered_records += observations.len() as u64;
            total.offered_bytes += observations
                .iter()
                .map(|(_, body)| body.map_or(0, <[u8]>::len) as u64)
                .sum::<u64>();
        }
        Ok(())
    }

    pub(in super::super) fn report_write_view(&self) {
        if !laststore::commit_timing::enabled() {
            return;
        }
        let Some(view) = &self.write_view else { return };
        if let Ok(counts) = view.counts.lock() {
            laststore::commit_timing::emit(
                "LASTDB_SNAPSHOT_TIMING",
                &serde_json::json!({
                    // Retain the wire kind for the existing CI comparison reader.
                    "kind": "write_reads", "owner": "common_write_view",
                    "started_us": view.started_us,
                    "ended_us": laststore::commit_timing::unix_us(),
                    "counts": &*counts,
                }),
            );
        }
    }
}
