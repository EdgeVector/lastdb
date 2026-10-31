//! Molecule drain wiring: the drain state, the ports it reads, and wrappers.

use std::sync::Arc;

use super::super::bounded_range::{RangeWalk, Step};
use super::super::counter::ReleaseRequest;
use super::super::molecule_drain::{
    DrainCursor, DrainLimits, DrainOutcome, DrainState, DrainingMolecules, MoleculeDrain,
    MoleculeDrainReport, MoleculeDrainVerdict,
};
use super::super::ports::{BindingReads, DrainStates, RangeWalks, RetentionReleases};
use super::super::retention::SlotReleases;
use super::super::{AtomStore, MoleculeBindings};
use crate::schema::SchemaError;

impl RangeWalks for AtomStore {
    async fn walk_step(&self, walk: &mut RangeWalk) -> Result<Step, SchemaError> {
        Self::walk_step(self, walk).await
    }
}

impl BindingReads for AtomStore {
    async fn read_binding_counters(
        &self,
        molecules: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Option<MoleculeBindings>>, SchemaError> {
        Self::read_binding_counters(self, molecules, storage_prefix).await
    }

    async fn binding_record_present(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        Self::binding_record_present(self, molecule_uuid, storage_prefix).await
    }
}

impl RetentionReleases for AtomStore {
    async fn slot_release_requests(
        &self,
        scope: Option<&str>,
        removals: &[(&str, &[(String, String)])],
        preserve_archives: bool,
    ) -> Result<SlotReleases, SchemaError> {
        Self::slot_release_requests(self, scope, removals, preserve_archives).await
    }

    fn directory_release_requests(&self, rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<ReleaseRequest> {
        Self::directory_release_requests(self, rows)
    }
}

impl DrainStates for AtomStore {
    fn drain_state(&self) -> &DrainState {
        &self.molecule_drain
    }
}

impl AtomStore {
    /// Check the scope that a verb names. The only scopes a verb may name are the
    /// personal scope (no value) and a database scope: exactly 64 lowercase hex
    /// characters. A mistyped scope reads an empty range and looks like a clean
    /// scope, so the verbs that take a scope check it before they read. The check is
    /// narrower than the key validation of the storage layer on purpose: a verb
    /// never needs a `cloud:` or `from:` scope or a reserved name.
    pub fn validate_scope(scope: Option<&str>) -> Result<(), SchemaError> {
        super::super::helpers::validate_scope(scope)
    }

    /// The report of the drain and the wake signal of its task.
    pub(crate) fn drain_state(&self) -> &DrainState {
        &self.molecule_drain
    }

    /// The drain report now.
    #[must_use]
    pub fn molecule_drain_report(&self) -> MoleculeDrainReport {
        self.molecule_drain.snapshot()
    }

    /// The wake signal, for the task.
    pub(crate) fn drain_wake_signal(&self) -> Arc<tokio::sync::Notify> {
        self.molecule_drain.wake_signal()
    }

    /// Wake the drain task when the batch of the current request is committed.
    pub(crate) fn wake_drain_after_commit(&self) {
        MoleculeDrain::new(self).wake_drain_after_commit();
    }

    /// Read the molecule inside the write gate and queue it for the drain.
    pub async fn molecule_drain_enqueue(
        &self,
        scope: Option<&str>,
        molecule: &str,
    ) -> Result<MoleculeDrainVerdict, SchemaError> {
        MoleculeDrain::new(self)
            .molecule_drain_enqueue(scope, molecule)
            .await
    }

    /// The molecules of `molecules` that the drain still removes in `scope`.
    pub(crate) async fn molecules_draining(
        &self,
        scope: Option<&str>,
        molecules: &[String],
    ) -> Result<DrainingMolecules, SchemaError> {
        MoleculeDrain::new(self)
            .molecules_draining(scope, molecules)
            .await
    }

    /// Whether the drain task is off (`LASTDB_MOLECULE_DRAIN_MS=0`).
    pub(crate) fn drain_task_is_off(&self) -> bool {
        MoleculeDrain::new(self).drain_task_is_off()
    }

    /// The candidate rows now, counted up to `cap`.
    pub async fn molecule_drain_candidates(&self, cap: usize) -> Result<(u64, bool), SchemaError> {
        MoleculeDrain::new(self)
            .molecule_drain_candidates(cap)
            .await
    }

    /// Run one drain step.
    pub(crate) async fn molecule_drain_step(
        &self,
        cursor: &mut DrainCursor,
        limits: DrainLimits,
    ) -> Result<Option<DrainOutcome>, SchemaError> {
        MoleculeDrain::new(self)
            .molecule_drain_step(cursor, limits)
            .await
    }
}
