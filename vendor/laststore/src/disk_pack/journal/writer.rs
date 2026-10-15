use super::batch::BatchPlan;
use super::marker::Marker;
use super::{JournalMutation, JournalTail};
use crate::disk_pack::bytes::digest;
use crate::disk_pack::pack_write::PackWriter;
use crate::disk_pack::page_write::directory;
use crate::disk_pack::{corrupt, Current, Location, MutationVersion, Source};
use crate::read_receipt_io::{self as measured, ReadClass};
use crate::Result;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use uuid::Uuid;

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

/// Exclusive journal constructor for a separate artifact with no prior journal.
/// Failed file publication poisons this writer. It never reuses a partial batch.
pub struct JournalWriter {
    root: PathBuf,
    current: Current,
    tail: JournalTail,
    poisoned: bool,
    reserved_bytes: u64,
    _lock: File,
}

pub(super) fn artifact_current(root: &Path) -> Result<Current> {
    let expected = b"artifact-only=1\nformat=1\n";
    let _class = ReadClass::FormatProbe.enter();
    let mut marker = measured::open(root.join("disk-pack-artifact-v1"))?;
    if marker.metadata()?.len() != expected.len() as u64 {
        return Err(super::refuse("not a separate artifact"));
    }
    let mut bytes = vec![0; expected.len()];
    marker.read_exact(&mut bytes)?;
    if bytes != expected {
        return Err(super::refuse("not a separate artifact"));
    }
    let mut file = measured::open(root.join("CURRENT"))?;
    if file.metadata()?.len() != 4096 {
        return Err(corrupt("CURRENT length"));
    }
    let mut bytes = [0; 4096];
    file.read_exact(&mut bytes)?;
    Current::decode(&bytes)
}

impl JournalWriter {
    /// Create one new stream for the artifact's CURRENT. Resume and stream rotation
    /// require the future root publisher and are explicitly refused here.
    pub fn create(root: &Path) -> Result<Self> {
        let current = artifact_current(root)?;
        if current.required_features & crate::disk_pack::FEATURE_OBJECT_METADATA == 0 {
            return Err(super::refuse(
                "new journal writes require complete object bounds",
            ));
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join("journal").join("LOCK"))?;
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(std::io::Error::from)?;
        if root.join("JOURNAL_TAIL").try_exists()? {
            return Err(super::refuse("artifact already has a journal"));
        }
        directory(
            &root
                .join("journal")
                .join(current.journal_stream.to_string()),
        )?;
        let tail = JournalTail::initial(&current)?;
        tail.publish(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            current,
            tail,
            poisoned: false,
            reserved_bytes: 0,
            _lock: lock,
        })
    }

    /// Current durable boundary; an ordinary read still uses CURRENT.
    pub fn tail(&self) -> &JournalTail {
        &self.tail
    }

    /// Worst-case uncompressed part/map bytes retained after the artifact's CURRENT.
    pub fn reserved_suffix_bytes(&self) -> u64 {
        self.reserved_bytes
    }

    /// Append one complete bounded batch, then publish TAIL after every file barrier.
    /// This call does not publish directory roots or release protected write records.
    pub fn append(
        &mut self,
        mutations: Vec<JournalMutation>,
        accepted_ms: u64,
    ) -> Result<DurableBatch> {
        if self.poisoned {
            return Err(super::refuse(
                "writer requires recovery after failed publication",
            ));
        }
        let plan = BatchPlan::new(&mutations)?;
        if self.tail.last_batch - (self.tail.first_batch - 1) >= super::MAX_SUFFIX_BATCHES
            || self
                .reserved_bytes
                .checked_add(plan.physical_reserve)
                .and_then(|n| n.checked_add(8192))
                .map_or(true, |n| n > super::MAX_SUFFIX_BYTES)
        {
            return Err(super::refuse("journal suffix admission limit"));
        }
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
        let result = self.append_prepared(mutations, marker, generation, &plan)?;
        self.reserved_bytes += plan.physical_reserve;
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
        let begin = marker.encode(false)?;
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
        let commit = marker.encode(true)?;
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
