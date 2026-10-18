use super::batch::BatchPlan;
use super::marker::Marker;
use super::{JournalMutation, JournalTail};
use crate::disk_pack::bytes::digest;
use crate::disk_pack::pack_write::PackWriter;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{corrupt, Current, Location, MutationVersion, PublishedGeneration, Source};
use crate::Result;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

#[path = "writer_maintenance.rs"]
mod maintenance;

/// A complete journal batch with durable recovery bytes and exact record extents.
pub struct DurableBatch {
    /// Complete mutation batch sequence.
    pub sequence: u64,
    /// One direct location for each input operation, including tombstones.
    pub locations: Vec<Location>,
    /// Actual complete data-part and map bytes.
    pub encoded_bytes: u64,
    /// Complete COMMIT marker digest recorded by TAIL.
    pub marker_digest: [u8; 32],
}

/// Exclusive journal writer for a separate artifact, with bounded restart recovery.
/// Failed file publication poisons this writer. It never reuses a partial batch.
pub struct JournalWriter {
    root: PathBuf,
    current: Current,
    tail: JournalTail,
    poisoned: bool,
    reserved_bytes: u64,
    lock: Arc<WriteLock>,
    admission: Arc<super::admission::AdmissionLedger>,
    suffix_oldest: Option<Instant>,
}

impl JournalWriter {
    /// Create the first stream only. Existing journals require `open`.
    pub fn create(root: &Path) -> Result<Self> {
        let lock = WriteLock::acquire(root)?;
        let current = crate::disk_pack::artifact::current(lock.root())?;
        if lock.root().join("JOURNAL_TAIL").try_exists()? {
            return Err(super::refuse("artifact already has a journal"));
        }
        super::resume::initial(&lock, &current)?;
        let (publication, tail) = super::resume::checkpoint(&lock)?;
        Ok(Self::from_state(lock, publication.current, tail))
    }

    /// Recover every complete pending batch and start a fresh stream before admission.
    /// An existing stream without TAIL is an availability error, never an empty guess.
    pub fn open(root: &Path) -> Result<Self> {
        let lock = WriteLock::acquire(root)?;
        Self::open_locked(lock)
    }

    pub(crate) fn open_conversion(root: &Path, token: Uuid) -> Result<Self> {
        Self::open_locked(WriteLock::acquire_conversion(root, token)?)
    }

    fn open_locked(lock: WriteLock) -> Result<Self> {
        let current = crate::disk_pack::artifact::current(lock.root())?;
        if !lock.root().join("JOURNAL_TAIL").try_exists()? {
            super::resume::initial(&lock, &current)?;
        }
        let (publication, tail) = super::resume::checkpoint(&lock)?;
        Ok(Self::from_state(lock, publication.current, tail))
    }

    fn from_state(lock: WriteLock, current: Current, tail: JournalTail) -> Self {
        Self {
            root: lock.root().to_path_buf(),
            current,
            tail,
            poisoned: false,
            reserved_bytes: 0,
            lock: Arc::new(lock),
            admission: Arc::default(),
            suffix_oldest: None,
        }
    }

    /// Last complete directory boundary. No uncommitted suffix is represented here.
    pub fn current(&self) -> &Current {
        &self.current
    }

    /// Publish all complete pending batches and rotate only after a successful checkpoint.
    /// No-op calls keep the generation. Any failure poisons write admission.
    pub fn publish(&mut self) -> Result<PublishedGeneration> {
        if self.poisoned {
            return Err(super::refuse(
                "writer requires recovery after failed publication",
            ));
        }
        self.poisoned = true;
        if self.tail.last_batch == self.current.frontier {
            let publication = crate::disk_pack::publish::publish_locked(&self.lock)?;
            self.current = publication.current.clone();
            self.poisoned = false;
            return Ok(publication);
        }
        let (publication, tail) = super::resume::checkpoint(&self.lock)?;
        self.current = publication.current.clone();
        self.tail = tail;
        self.reserved_bytes = 0;
        self.suffix_oldest = None;
        self.poisoned = false;
        Ok(publication)
    }

    /// Current durable boundary; an ordinary read still uses CURRENT.
    pub fn tail(&self) -> &JournalTail {
        &self.tail
    }

    /// Worst-case uncompressed part/map bytes retained after the artifact's CURRENT.
    pub fn reserved_suffix_bytes(&self) -> u64 {
        self.reserved_bytes
    }

    /// Validate bounded future directory growth before an accepted mutation.
    /// A native caller owns its exclusive complete-write gate through publish.
    pub(crate) fn check_dependency_capacity(&self, additional: u64) -> Result<()> {
        self.check_admission()?;
        if self.tail.last_batch != self.current.frontier {
            return Err(super::refuse(
                "complete admission requires a published suffix",
            ));
        }
        let generation = crate::disk_pack::publish::ensure_generation(&self.lock, &self.current)?;
        if generation
            .dependencies
            .checked_add(additional)
            .map_or(true, |parts| {
                parts > crate::disk_pack::dependency::MAX_DEPENDENCIES as u64
            })
        {
            return Err(super::capacity(
                "complete mutation exceeds dependency capacity",
            ));
        }
        Ok(())
    }

    /// Reserve a complete journal batch before its owned payloads move.
    /// This grants no directory capacity, logical-record protection, or ack.
    pub fn reserve(&self, mutations: &[JournalMutation]) -> Result<super::JournalReservation> {
        self.reserve_quote(super::JournalQuote::from_plan(
            mutations.len(),
            &BatchPlan::new(mutations, self.current.batch_operation_limit())?,
        ))
    }

    /// Reserve checked dimensions without retaining or copying the body.
    pub fn reserve_quote(&self, quote: super::JournalQuote) -> Result<super::JournalReservation> {
        self.reserve_quote_at(quote, Instant::now())
    }

    pub(in crate::disk_pack) fn reserve_quote_extension(
        &self,
        previous: &super::JournalReservation,
        quote: super::JournalQuote,
    ) -> Result<super::JournalReservation> {
        previous.validate(&self.admission, previous.quote())?;
        self.reserve_quote_at(quote, previous.created())
    }

    fn reserve_quote_at(
        &self,
        quote: super::JournalQuote,
        created: Instant,
    ) -> Result<super::JournalReservation> {
        self.check_admission()?;
        if quote.operations() > self.current.batch_operation_limit() {
            return Err(super::refuse(
                "batch exceeds held CURRENT operation capability",
            ));
        }
        self.admission.reserve(
            quote,
            self.reserved_bytes,
            self.tail.last_batch - (self.tail.first_batch - 1),
            self.suffix_oldest,
            created,
        )
    }

    /// Report live ticket capacity in addition to the durable suffix.
    pub fn pressure(&self) -> Result<super::JournalPressure> {
        self.admission
            .pressure(self.reserved_bytes, self.suffix_oldest)
    }

    /// Append one complete bounded batch, then publish TAIL after every file barrier.
    /// This call does not publish directory roots or release protected write records.
    pub fn append(
        &mut self,
        mutations: Vec<JournalMutation>,
        accepted_ms: u64,
    ) -> Result<DurableBatch> {
        let reservation = self.reserve(&mutations)?;
        self.append_reserved(mutations, accepted_ms, reservation)
    }

    /// Consume only a ticket issued by this writer with sufficient dimensions.
    pub fn append_reserved(
        &mut self,
        mutations: Vec<JournalMutation>,
        accepted_ms: u64,
        mut reservation: super::JournalReservation,
    ) -> Result<DurableBatch> {
        self.check_admission()?;
        let plan = BatchPlan::new(&mutations, self.current.batch_operation_limit())?;
        reservation.validate(
            &self.admission,
            super::JournalQuote::from_plan(mutations.len(), &plan),
        )?;
        self.append_admitted(mutations, accepted_ms, &mut reservation, plan)
    }

    fn check_admission(&self) -> Result<()> {
        if self.poisoned {
            return Err(super::refuse(
                "writer requires recovery after failed publication",
            ));
        }
        if self.current.required_features & crate::disk_pack::FEATURE_JOURNAL_LIFECYCLE == 0
            || self.current.previous_generation == 0
        {
            return Err(super::refuse(
                "journal admission requires a complete initial stream checkpoint",
            ));
        }
        Ok(())
    }

    fn append_admitted(
        &mut self,
        mutations: Vec<JournalMutation>,
        accepted_ms: u64,
        reservation: &mut super::JournalReservation,
        plan: BatchPlan,
    ) -> Result<DurableBatch> {
        let sequence = self
            .tail
            .last_batch
            .checked_add(1)
            .ok_or_else(|| corrupt("journal sequence exhaustion"))?;
        let generation = self
            .current
            .generation
            .checked_add(1)
            .ok_or_else(|| corrupt("generation exhaustion"))?;
        let marker = Marker {
            operations: mutations.len() as u32,
            sequence,
            previous: self.tail.last_batch,
            stream: self.tail.stream,
            logical_bytes: plan.logical_bytes,
            accepted_ms,
            batch_digest: [0; 32],
        };
        self.poisoned = true;
        reservation.begin()?;
        let result = self.append_prepared(mutations, marker, generation, &plan)?;
        self.reserved_bytes += plan.physical_reserve;
        self.suffix_oldest = Some(self.suffix_oldest.map_or(reservation.created(), |oldest| {
            oldest.min(reservation.created())
        }));
        reservation.complete()?;
        self.poisoned = false;
        Ok(result)
    }

    fn append_prepared(
        &mut self,
        mutations: Vec<JournalMutation>,
        mut marker: Marker,
        generation: u64,
        plan: &BatchPlan,
    ) -> Result<DurableBatch> {
        let mut locations = Vec::new();
        locations
            .try_reserve_exact(mutations.len())
            .map_err(|_| super::refuse("location buffer admission failed"))?;
        let mut data = PackWriter::journal(&self.root, marker.stream, marker.sequence, generation)?;
        let begin = marker.encode(false, self.current.batch_operation_limit())?;
        data.append_raw(&begin)?;
        let mut hash = Sha256::new();
        hash.update(begin);
        for (number, mutation) in mutations.iter().enumerate() {
            locations.push(write_operation(
                &mut data,
                &mut hash,
                mutation,
                marker.sequence,
                number as u32,
            )?);
        }
        marker.batch_digest = hash.finalize().into();
        if data.position().checked_add(128) != Some(marker.logical_bytes) {
            return Err(corrupt(
                "journal COMMIT position differs from complete length",
            ));
        }
        let commit = marker.encode(true, self.current.batch_operation_limit())?;
        data.append_raw(&commit)?;
        let written = data.finish()?;
        if written.logical_bytes != plan.logical_bytes
            || written.physical_bytes > plan.physical_reserve
        {
            return Err(corrupt(
                "journal output exceeded its complete batch reserve",
            ));
        }
        let marker_digest = digest(&commit);
        let next = JournalTail {
            stream: marker.stream,
            first_batch: self.tail.first_batch,
            last_batch: marker.sequence,
            encoded_bytes: self
                .tail
                .encoded_bytes
                .checked_add(written.physical_bytes)
                .ok_or_else(|| corrupt("TAIL byte count overflow"))?,
            published_ms: unix_ms(),
            last_marker_digest: marker_digest,
            predecessor: None,
        };
        next.publish(&self.root)?;
        self.tail = next;
        Ok(DurableBatch {
            sequence: marker.sequence,
            locations,
            encoded_bytes: written.physical_bytes,
            marker_digest,
        })
    }
}

fn write_operation(
    data: &mut PackWriter,
    hash: &mut Sha256,
    mutation: &JournalMutation,
    sequence: u64,
    number: u32,
) -> Result<Location> {
    let encoded = mutation.encode(sequence, number)?;
    hash.update(encoded.header);
    mutation.hash_suffix(hash, encoded.record.as_ref());
    data.append_raw(&encoded.header)?;
    data.append_raw(mutation.collection.as_bytes())?;
    data.append_raw(mutation.key.as_bytes())?;
    let version = MutationVersion {
        batch: sequence,
        operation: number,
    };
    match &mutation.body {
        Some(body) => data.put(
            &mutation.collection,
            &mutation.key,
            version,
            body.len() as u64,
            &mut body.as_slice(),
        ),
        None => Ok(Location {
            source: Source::Tombstone,
            version,
            record_bytes: 0,
            object: Uuid::nil(),
            first_block: 0,
            first_position: 0,
            blocks: 0,
            digest: [0; 32],
        }),
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| u64::try_from(time.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
