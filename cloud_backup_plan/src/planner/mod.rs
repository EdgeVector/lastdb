//! Byte-range planner. File length never decides an allocation: each requested
//! range is at most [`READ_BYTES`], and this crate performs no file access.
use super::{object_receipt, ClientExtent, ClientNativeFile, PlanError, MAX_FILE_EXTENTS};
use cloud_backup_protocol::v2::Object;
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest};
use lastdb_storage_policy::backup_transport::{
    MAX_PACK_MEMBERS, METADATA_PACK_BYTES, OBJECT_CHUNK_BYTES, SMALL_NATIVE_FILE_BYTES,
};
use sha2::{Digest as _, Sha256};

mod pack;
mod span;

/// Every requested range is at most this many bytes.
pub const READ_BYTES: u64 = 65536;

/// One frozen native file, in plan order.
#[derive(Clone, Debug)]
pub struct PlanFile {
    /// The caller's file id. Ranges, segments and file records name this id.
    pub id: usize,
    pub path: cloud_backup_protocol::RelativePath,
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
    /// The ranges of the object bytes, in order.
    pub segments: Vec<PlanSegment>,
    /// A small-file pack: its first file is under the small-file size.
    pub pack: bool,
    /// Each file whose last byte is in this object, with all its extents.
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
    /// Deliver `bytes`, or report the caller's own access error.
    /// A shorter slice means the file ended when this is false.
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
    files: Vec<PlanFile>,
    scope: CloudScope,
    totals: PlanTotals,
    phase: Phase,
    awaiting: Option<Range>,
    save: Option<Pending>,
}

#[derive(Debug)]
enum Phase {
    Choose(usize),
    Pack(Box<Pack>),
    Span(Box<Span>),
    /// The last chunk of a large file was saved. One trailing byte remains.
    Tail(usize),
    Finished,
}

#[derive(Debug)]
struct Pack {
    start: usize,
    end: usize,
    file: usize,
    at: u64,
    object_at: u64,
    object_hash: Sha256,
    file_hash: Sha256,
    members: Vec<Member>,
}

#[derive(Debug)]
struct Member {
    file: usize,
    sha256: Digest,
    offset: u64,
}

#[derive(Debug)]
struct Span {
    file: usize,
    base: u64,
    filled: u64,
    length: u64,
    object_hash: Sha256,
    file_hash: Sha256,
    extents: Vec<ClientExtent>,
}

#[derive(Debug)]
struct Pending {
    index: u64,
    object: PlannedObject,
}

impl PlanState {
    /// Start a plan over `files` in order. The same frozen files give the same
    /// objects in the same order, so a resumed plan can compare.
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
            Phase::Pack(_) => pack::absorb(self, bytes),
            Phase::Span(_) => span::absorb(self, bytes),
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
            pack::finish_member(self)
        }
    }

    fn stage(&mut self, object: PlannedObject, phase: Phase) {
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

impl Pack {
    fn open(start: usize, end: usize) -> Self {
        Self {
            start,
            end,
            file: start,
            at: 0,
            object_at: 0,
            object_hash: Sha256::new(),
            file_hash: Sha256::new(),
            members: Vec::new(),
        }
    }

    fn range(&self, files: &[PlanFile]) -> Range {
        let file = &files[self.file];
        if self.at == file.bytes {
            probe(file)
        } else {
            Range {
                file: file.id,
                offset: self.at,
                bytes: READ_BYTES.min(file.bytes - self.at),
                exact: false,
            }
        }
    }
}

impl Span {
    fn open(file: usize, bytes: u64) -> Self {
        Self {
            file,
            base: 0,
            filled: 0,
            length: OBJECT_CHUNK_BYTES.min(bytes),
            object_hash: Sha256::new(),
            file_hash: Sha256::new(),
            extents: Vec::new(),
        }
    }

    fn range(&self, files: &[PlanFile]) -> Range {
        Range {
            file: files[self.file].id,
            offset: self.base + self.filled,
            bytes: READ_BYTES.min(self.length - self.filled),
            exact: true,
        }
    }
}

fn probe(file: &PlanFile) -> Range {
    Range {
        file: file.id,
        offset: file.bytes,
        bytes: 1,
        exact: false,
    }
}

fn digest(hash: Sha256) -> Result<Digest, PlanError> {
    let hex: String = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(Digest::try_from(hex)?)
}

fn receipt(scope: &CloudScope, object: &Object) -> Result<Digest, PlanError> {
    Ok(object_receipt(scope, object)
        .map_err(PlanError::Protocol)?
        .receipt_id)
}

fn extent(receipt_id: Digest, length: u64) -> ClientExtent {
    ClientExtent {
        receipt_id,
        offset: DecimalU64::new(0),
        length: DecimalU64::new(length),
    }
}

fn bounded(extents: &[ClientExtent]) -> Result<(), PlanError> {
    if extents.len() > MAX_FILE_EXTENTS {
        Err(PlanError::ExtentBound)
    } else {
        Ok(())
    }
}
