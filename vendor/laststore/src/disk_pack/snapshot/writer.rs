use super::admission::AdmissionLedger;
use super::{PreparedMutation, SnapshotMutation, SnapshotQuote, SnapshotReservation};
use crate::disk_pack::live_parts::LiveParts;
use crate::disk_pack::pack_write::PackWriter;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{
    artifact, corrupt, publish, Current, Location, MutationVersion, PublishedGeneration, Source,
};
use crate::{durability, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// Exclusive direct writer. No pending disk output becomes recovery input.
pub struct SnapshotWriter {
    pub(super) root: PathBuf,
    pub(super) current: Current,
    pub(super) poisoned: bool,
    pub(super) dependencies: u64,
    pub(super) legacy_live: Option<LiveParts>,
    pub(in crate::disk_pack) lock: Arc<WriteLock>,
    admission: Arc<AdmissionLedger>,
    pub(super) compaction: crate::disk_pack::maintenance::CompactionPolicy,
}

impl SnapshotWriter {
    /// Open only the selected complete generation. Unreferenced output stays unused.
    pub fn open(root: &Path) -> Result<Self> {
        Self::open_locked(WriteLock::acquire(root)?)
    }

    pub(crate) fn open_conversion(root: &Path, token: Uuid) -> Result<Self> {
        Self::open_locked(WriteLock::acquire_conversion(root, token)?)
    }

    fn open_locked(lock: WriteLock) -> Result<Self> {
        let current = artifact::current(lock.root())?;
        let receipt = publish::ensure_generation(&lock, &current)?;
        // Legacy metadata work and its resource checks precede every memory ack.
        // The first commit transfers these counts into the counted receipt.
        let legacy_live =
            if current.required_features & crate::disk_pack::FEATURE_LIVE_PART_COUNTS == 0 {
                Some(LiveParts::bootstrap(
                    lock.root(),
                    &current,
                    &receipt.coordinates,
                )?)
            } else {
                None
            };
        Ok(Self {
            root: lock.root().to_path_buf(),
            current,
            poisoned: false,
            dependencies: receipt.dependencies,
            legacy_live,
            lock: Arc::new(lock),
            admission: Arc::default(),
            compaction: Default::default(),
        })
    }

    /// Complete selected root; no uncommitted suffix exists.
    pub fn current(&self) -> &Current {
        &self.current
    }
    /// Number of retained dependency parts in the selected receipt.
    pub fn dependency_parts(&self) -> u64 {
        self.dependencies
    }
    /// An uncertain publication requires a reopen before more writes.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub(super) fn check_admission(&self) -> Result<()> {
        if self.poisoned {
            return Err(super::refuse(
                "writer requires reopen after an uncertain commit",
            ));
        }
        Ok(())
    }

    pub(crate) fn check_dependency_capacity(&self, additional: u64) -> Result<()> {
        self.check_admission()?;
        if self.dependencies.saturating_add(additional) > crate::disk_pack::MAX_DEPENDENCY_PARTS {
            return Err(crate::Error::WriteCapacity(format!(
                "complete mutation exceeds dependency capacity: {} existing parts plus {additional}", self.dependencies
            )));
        }
        Ok(())
    }

    pub(crate) fn reserve_quote(&self, quote: SnapshotQuote) -> Result<SnapshotReservation> {
        self.check_admission()?;
        self.admission.reserve(quote, Instant::now(), None)
    }

    pub(crate) fn reserve_quote_extension(
        &self,
        previous: &SnapshotReservation,
        quote: SnapshotQuote,
    ) -> Result<SnapshotReservation> {
        self.check_admission()?;
        previous.validate(&self.admission, previous.quote())?;
        self.admission
            .reserve(quote, previous.created(), Some(previous))
    }

    /// Commit one complete batch with one receipt and root, then finish durability.
    pub fn commit_reserved(
        &mut self,
        mutations: Vec<SnapshotMutation>,
        reservation: SnapshotReservation,
    ) -> Result<PublishedGeneration> {
        self.commit_reserved_base(mutations, reservation, None)
    }

    pub(super) fn commit_reserved_base(
        &mut self,
        mutations: Vec<SnapshotMutation>,
        mut reservation: SnapshotReservation,
        base: Option<super::merge::MergePublication<'_>>,
    ) -> Result<PublishedGeneration> {
        self.check_admission()?;
        let actual = SnapshotQuote::from_iter_with_limit(
            mutations.iter().map(SnapshotMutation::shape),
            self.current.batch_operation_limit(),
        )?;
        reservation.validate(&self.admission, actual)?;
        reservation.begin()?;
        self.poisoned = true;
        let root = self.root.clone();
        let publication =
            durability::complete_native_commit(&root, || self.commit(mutations, actual, base))?;
        reservation.complete()?;
        self.current = publication.current.clone();
        self.dependencies = publication.dependency_parts;
        self.poisoned = false;
        Ok(publication)
    }

    fn commit(
        &mut self,
        mutations: Vec<SnapshotMutation>,
        quote: SnapshotQuote,
        base: Option<super::merge::MergePublication<'_>>,
    ) -> Result<PublishedGeneration> {
        let mut proposed = self.current.clone();
        proposed.generation = publish::fresh_generation(&self.root, self.current.generation)?;
        proposed.previous_generation = self.current.generation;
        proposed.frontier = self
            .current
            .frontier
            .checked_add(1)
            .ok_or_else(|| corrupt("snapshot commit sequence exhaustion"))?;
        proposed.published_ms = artifact::unix_ms();
        let payload_timing = crate::commit_timing::stage("payload_us");
        let mut output = None;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(mutations.len())
            .map_err(|_| super::super::resource())?;
        for (number, mutation) in mutations.into_iter().enumerate() {
            let version = MutationVersion {
                batch: proposed.frontier,
                operation: number as u32,
            };
            let location = if let Some(body) = mutation.body {
                if output.is_none() {
                    output = Some(PackWriter::new(&self.root, proposed.generation)?);
                }
                output.as_mut().expect("created payload writer").put(
                    &mutation.collection,
                    &mutation.key,
                    version,
                    body.len() as u64,
                    &mut body.as_slice(),
                )?
            } else {
                Location {
                    source: Source::Tombstone,
                    version,
                    record_bytes: 0,
                    object: Uuid::nil(),
                    first_block: 0,
                    first_position: 0,
                    blocks: 0,
                    digest: [0; 32],
                }
            };
            prepared.push(PreparedMutation {
                collection: mutation.collection,
                group: mutation.group,
                key: mutation.key,
                location,
            });
        }
        let payload = output.map(PackWriter::finish).transpose()?;
        if payload.as_ref().is_some_and(|data| {
            data.logical_bytes > quote.logical_bytes()
                || data.physical_bytes > quote.physical_reserve()
        }) {
            return Err(corrupt("snapshot payload exceeded its admitted dimensions"));
        }
        drop(payload_timing);
        if let Some(base) = base {
            prepared
                .try_reserve_exact(base.delta.mutations.len())
                .map_err(|_| super::super::resource())?;
            prepared.extend(base.delta.mutations);
            return publish::publish_prepared_base(
                &self.lock,
                &self.current,
                proposed,
                prepared,
                payload,
                base.output.publication_base(),
            );
        }
        let legacy_live = self.legacy_live.take();
        publish::publish_prepared(
            &self.lock,
            &self.current,
            proposed,
            prepared,
            payload,
            legacy_live,
        )
    }
}
