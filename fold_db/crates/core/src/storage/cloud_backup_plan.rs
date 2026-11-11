//! The one upload planner for native LastDB cloud backups.

use cloud_backup_protocol::v2::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::v2::{Object, Version};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, ProtocolError, RelativePath};
use lastdb_model::storage_policy::backup_transport::{
    MAX_PACK_MEMBERS, METADATA_PACK_BYTES, OBJECT_CHUNK_BYTES, SMALL_NATIVE_FILE_BYTES,
};
use serde::{Deserialize, Serialize};

mod steps;
use steps::{probe, Pack, Span};

pub const MAX_FILE_EXTENTS: usize = 16384;
/// Every requested range is at most this many bytes.
pub const READ_BYTES: u64 = 65536;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientExtent {
    pub receipt_id: Digest,
    pub offset: DecimalU64,
    pub length: DecimalU64,
}

/// One native file and the object ranges that hold its bytes, in order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientNativeFile {
    pub path: RelativePath,
    pub native_sha256: Digest,
    pub native_bytes: DecimalU64,
    pub extents: Vec<ClientExtent>,
}

impl ClientNativeFile {
    pub fn validate(&self) -> Result<(), PlanError> {
        if self.native_bytes.get() == 0
            || self.extents.is_empty()
            || self.extents.len() > MAX_FILE_EXTENTS
        {
            return Err(PlanError::Invalid("byte native file extent count"));
        }
        let mut bytes = 0u64;
        for extent in &self.extents {
            if extent.length.get() == 0
                || extent
                    .offset
                    .get()
                    .checked_add(extent.length.get())
                    .is_none()
            {
                return Err(PlanError::Invalid("byte native extent bounds"));
            }
            bytes = bytes
                .checked_add(extent.length.get())
                .ok_or(PlanError::Invalid("byte native extent overflow"))?;
        }
        if bytes != self.native_bytes.get() {
            return Err(PlanError::Invalid("byte native file exact length"));
        }
        Ok(())
    }
}

/// A refusal from the planner. Each caller maps it to its own error.
#[derive(Debug)]
pub enum PlanError {
    /// A file record is out of bounds. The text names the bound.
    Invalid(&'static str),
    /// A frozen file holds fewer or more bytes than its frozen length.
    SourceLength,
    /// A file needs more extents than one record holds.
    ExtentBound,
    Protocol(ProtocolError),
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => f.write_str(reason),
            Self::SourceLength => f.write_str("byte native source length changed"),
            Self::ExtentBound => f.write_str("byte native file extent metadata bound"),
            Self::Protocol(_) => f.write_str("byte protocol validation"),
        }
    }
}

impl std::error::Error for PlanError {}

impl From<ProtocolError> for PlanError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

/// The receipt the service gives for `object` in `scope`.
pub fn object_receipt(scope: &CloudScope, object: &Object) -> Result<ObjectReceipt, ProtocolError> {
    let mut receipt = ObjectReceipt {
        version: Version::V2,
        receipt_id: object.sha256.clone(),
        scope: scope.clone(),
        object: object.clone(),
    };
    receipt.receipt_id = receipt.identity_sha256()?;
    receipt.validate()?;
    Ok(receipt)
}

/// The receipt that confirms one transfer input.
pub fn expected_receipt(input: &TransferInput) -> Result<ObjectReceipt, ProtocolError> {
    input.validate()?;
    object_receipt(&input.job.stream.scope, &input.object)
}

/// One frozen native file, in plan order.
#[derive(Clone, Debug)]
pub struct PlanFile {
    pub id: usize,
    pub path: RelativePath,
    pub bytes: u64,
    pub destination: cloud_backup_protocol::v2::Destination,
}

/// One byte range of one frozen file inside an object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanSegment {
    pub file: usize,
    pub offset: u64,
    pub bytes: u64,
}

/// One transfer object, before the caller gives it a request.
#[derive(Clone, Debug)]
pub struct PlannedObject {
    pub object: Object,
    pub segments: Vec<PlanSegment>,
    pub pack: bool,
    pub native_files: Vec<(usize, ClientNativeFile)>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlanTotals {
    pub objects: u64,
    pub packs: u64,
    pub multi_file_packs: u64,
}

/// Bytes of one frozen file. `bytes` begins at `offset` in that file.
#[derive(Clone, Copy, Debug)]
pub struct Chunk<'a> {
    pub file: usize,
    pub offset: u64,
    pub bytes: &'a [u8],
}

/// The next range the plan needs from one frozen file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Range {
    pub file: usize,
    pub offset: u64,
    pub bytes: u64,
    pub exact: bool,
}

/// The plan's next effect. `step(None)` asks for one; `step(Some(chunk))`
/// answers a [`Step::Need`].
#[derive(Debug)]
pub enum Step {
    Save { index: u64, object: PlannedObject },
    Need(Range),
    Done(PlanTotals),
}

/// Plan state. `(state, chunk) -> (state, Save | Need | Done)`.
#[derive(Debug)]
pub struct PlanState {
    pub(super) files: Vec<PlanFile>,
    pub(super) scope: CloudScope,
    totals: PlanTotals,
    pub(super) phase: Phase,
    awaiting: Option<Range>,
    save: Option<Pending>,
}

#[derive(Debug)]
pub(super) enum Phase {
    Choose(usize),
    Pack(Box<Pack>),
    Span(Box<Span>),
    Tail(usize),
    Finished,
}

#[derive(Debug)]
struct Pending {
    index: u64,
    object: PlannedObject,
}

impl PlanState {
    /// Start a plan over `files` in order.
    pub fn start(files: &[PlanFile], scope: &CloudScope) -> Self {
        Self {
            files: files.to_vec(),
            scope: scope.clone(),
            totals: PlanTotals::default(),
            phase: Phase::Choose(0),
            awaiting: None,
            save: None,
        }
    }

    /// Advance one step. Pass `None` until the plan asks for bytes, then pass
    /// the delivered range. After [`Step::Save`], pass `None` again.
    pub fn step(&mut self, chunk: Option<Chunk<'_>>) -> Result<Step, PlanError> {
        if let Some(chunk) = chunk {
            self.consume(chunk)?;
        } else if self.awaiting.is_some() {
            return Err(PlanError::Invalid("byte plan range"));
        }
        if let Some(save) = self.save.take() {
            return Ok(Step::Save {
                index: save.index,
                object: save.object,
            });
        }
        self.kick()
    }

    fn consume(&mut self, chunk: Chunk<'_>) -> Result<(), PlanError> {
        let expect = self
            .awaiting
            .take()
            .ok_or(PlanError::Invalid("byte plan range"))?;
        if chunk.file != expect.file
            || chunk.offset != expect.offset
            || chunk.bytes.len() as u64 > expect.bytes
            || (expect.exact && chunk.bytes.len() as u64 != expect.bytes)
        {
            return Err(PlanError::Invalid("byte plan range"));
        }
        if self.probing() {
            if chunk.bytes.is_empty() {
                self.finish_probe()
            } else {
                Err(PlanError::SourceLength)
            }
        } else if chunk.bytes.len() as u64 != expect.bytes {
            Err(PlanError::SourceLength)
        } else {
            self.absorb(chunk.bytes)
        }
    }

    fn kick(&mut self) -> Result<Step, PlanError> {
        let choice = match &self.phase {
            Phase::Finished => return Ok(Step::Done(self.totals)),
            Phase::Choose(index) => Some(*index),
            Phase::Pack(_) | Phase::Span(_) | Phase::Tail(_) => None,
        };
        if let Some(index) = choice {
            self.open_next(index)
        } else {
            self.arm()
        }
    }

    fn open_next(&mut self, index: usize) -> Result<Step, PlanError> {
        if index >= self.files.len() {
            self.phase = Phase::Finished;
            return Ok(Step::Done(self.totals));
        }
        if self.files[index].bytes > OBJECT_CHUNK_BYTES {
            self.phase = Phase::Span(Box::new(Span::open(index, self.files[index].bytes)));
        } else {
            let end = self.pack_end(index);
            self.phase = Phase::Pack(Box::new(Pack::open(index, end)));
        }
        self.arm()
    }

    fn arm(&mut self) -> Result<Step, PlanError> {
        let range = self.next_range()?;
        self.awaiting = Some(range);
        Ok(Step::Need(range))
    }

    fn next_range(&self) -> Result<Range, PlanError> {
        match &self.phase {
            Phase::Pack(pack) => Ok(pack.range(&self.files)),
            Phase::Span(span) => Ok(span.range(&self.files)),
            Phase::Tail(file) => Ok(probe(&self.files[*file])),
            Phase::Choose(_) | Phase::Finished => Err(PlanError::Invalid("byte plan range")),
        }
    }

    fn probing(&self) -> bool {
        match &self.phase {
            Phase::Tail(_) => true,
            Phase::Pack(pack) => pack.at == self.files[pack.file].bytes,
            Phase::Span(_) | Phase::Choose(_) | Phase::Finished => false,
        }
    }

    fn absorb(&mut self, bytes: &[u8]) -> Result<(), PlanError> {
        match &self.phase {
            Phase::Pack(_) => steps::absorb_pack(self, bytes),
            Phase::Span(_) => steps::absorb_span(self, bytes),
            Phase::Choose(_) | Phase::Tail(_) | Phase::Finished => {
                Err(PlanError::Invalid("byte plan range"))
            }
        }
    }

    fn finish_probe(&mut self) -> Result<(), PlanError> {
        let tail = match &self.phase {
            Phase::Tail(file) => Some(*file),
            Phase::Pack(_) => None,
            Phase::Span(_) | Phase::Choose(_) | Phase::Finished => {
                return Err(PlanError::Invalid("byte plan range"));
            }
        };
        if let Some(file) = tail {
            self.phase = Phase::Choose(file + 1);
            Ok(())
        } else {
            steps::finish_pack_member(self)
        }
    }

    pub(super) fn stage(&mut self, object: PlannedObject, phase: Phase) {
        let index = self.totals.objects;
        self.totals.packs += u64::from(object.pack);
        self.totals.multi_file_packs += u64::from(object.pack && object.segments.len() > 1);
        self.totals.objects += 1;
        self.save = Some(Pending { index, object });
        self.phase = phase;
    }

    fn pack_end(&self, next: usize) -> usize {
        let first = &self.files[next];
        let mut end = next + 1;
        if first.bytes < SMALL_NATIVE_FILE_BYTES {
            let mut bytes = first.bytes;
            while let Some(file) = self.files.get(end) {
                if file.bytes >= SMALL_NATIVE_FILE_BYTES
                    || file.destination != first.destination
                    || end - next >= MAX_PACK_MEMBERS
                    || bytes
                        .checked_add(file.bytes)
                        .is_none_or(|n| n > METADATA_PACK_BYTES)
                {
                    break;
                }
                bytes += file.bytes;
                end += 1;
            }
        }
        end
    }
}
