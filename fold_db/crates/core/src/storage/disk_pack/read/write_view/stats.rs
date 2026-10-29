//! Fixed-name diagnostics for the common view; no keys or values enter logs.

use super::*;

#[derive(Default, serde::Serialize)]
pub(in super::super) struct WriteViewCounts {
    reused_records: u64,
    reused_bytes: u64,
    overlay_records: u64,
    shared_typed_records: u64,
    typed_fact_records: u64,
    typed_tips: u64,
    typed_atoms: u64,
    typed_metadata: u64,
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
    pub(in super::super) fn note_overlay(&mut self) {
        self.overlay_records += u64::from(laststore::commit_timing::enabled());
    }

    pub(in super::super) fn note(
        &mut self,
        coordinate: &Coordinate,
        body: Option<&[u8]>,
        owner: WriteOwner,
    ) {
        if !laststore::commit_timing::enabled() {
            return;
        }
        self.reused_records += 1;
        self.reused_bytes += body.map_or(0, <[u8]>::len) as u64;
        self.absences += u64::from(body.is_none());
        self.shared_typed_records += u64::from(!matches!(owner, WriteOwner::Fact));
        match owner {
            WriteOwner::Fact => self.typed_fact_records += 1,
            WriteOwner::Tip => self.typed_tips += 1,
            WriteOwner::Atom => self.typed_atoms += 1,
            WriteOwner::Definition => self.typed_metadata += 1,
        }
        match coordinate.0.as_str() {
            "tips" => {
                self.tips_and_controls += 1;
            }
            "atoms" if crate::atom::atom_key_codec::is_counter_key(&coordinate.1) => {
                self.counters += 1;
            }
            "atoms" => {
                self.atoms += 1;
            }
            _ => {
                self.metadata += 1;
            }
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
        total.shared_typed_records += counts.shared_typed_records;
        total.typed_fact_records += counts.typed_fact_records;
        total.typed_tips += counts.typed_tips;
        total.typed_atoms += counts.typed_atoms;
        total.typed_metadata += counts.typed_metadata;
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
                    "kind": "write_reads", "owner": "typed_common_write_view",
                    "started_us": view.started_us,
                    "ended_us": laststore::commit_timing::unix_us(),
                    "counts": &*counts,
                }),
            );
        }
    }
}
