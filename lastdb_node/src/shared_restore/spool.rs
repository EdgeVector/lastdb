//! Private bounded merge of newest-first replay windows.
//!
//! Runs are unlinked on open and encrypted under a session key. They are
//! reconstructible from the captured inputs and never become home files.

use super::StagedReplayWriter;
use crate::byte_cloud_restore::{RestoreError, Result};
use fold_db::crypto::{decrypt_envelope, encrypt_envelope};
use fold_db::portable_mutation::{MutationEntry, MAX_MUTATION_ENTRY_BYTES};
use rand::{rngs::OsRng, RngCore};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

const MAX_FRAME_BYTES: usize = MAX_MUTATION_ENTRY_BYTES + 64;
const MAX_APPLY_BYTES: usize = 4 * 1024 * 1024;
const MAX_APPLY_ENTRIES: usize = 128;
// A merge may hold original runs, its intermediate input and its output.
const MAX_RETAINED_BYTES: u64 = lastdb_storage_policy::BACKUP_RESOURCES.durable_spool_bytes / 3;

pub(super) struct CandidateSpool {
    root: PathBuf,
    key: Zeroizing<[u8; 32]>,
    runs: Vec<Option<Run>>,
    bytes: u64,
}

impl CandidateSpool {
    pub(super) fn new(root: &Path) -> Result<Self> {
        let mut key = Zeroizing::new([0; 32]);
        OsRng
            .try_fill_bytes(&mut *key)
            .map_err(|_| RestoreError::Invalid("spool key generation"))?;
        Ok(Self {
            root: root.to_path_buf(),
            key,
            runs: Vec::new(),
            bytes: 0,
        })
    }

    pub(super) fn push_window(&mut self, candidates: &[&MutationEntry]) -> Result<()> {
        if candidates.is_empty() {
            return Ok(());
        }
        let mut output = RunWriter::new(&self.root)?;
        for candidate in candidates {
            let plaintext = Zeroizing::new(
                candidate
                    .encode()
                    .map_err(|_| RestoreError::Invalid("candidate entry"))?,
            );
            let sealed = encrypt_envelope(&self.key, &plaintext)
                .map_err(|_| RestoreError::Invalid("spool encryption"))?;
            if self.bytes + output.bytes + frame_bytes(&sealed) > MAX_RETAINED_BYTES {
                return Err(RestoreError::Limit("shared replay spool"));
            }
            output.push(&sealed)?;
        }
        let mut run = output.finish()?;
        let bytes = self.bytes + run.bytes;
        let mut level = 0;
        // Keep original runs owned until the complete insertion succeeds.
        while let Some(prior) = self.runs.get_mut(level).and_then(Option::as_mut) {
            run = merge(&self.root, &self.key, prior, &mut run)?;
            level += 1;
        }
        for prior in &mut self.runs[..level] {
            *prior = None;
        }
        if level == self.runs.len() {
            self.runs.push(Some(run));
        } else {
            self.runs[level] = Some(run);
        }
        self.bytes = bytes;
        Ok(())
    }

    pub(super) async fn replay(mut self, writer: &mut impl StagedReplayWriter) -> Result<()> {
        let Some(mut run) = self.finish_run()? else {
            return Ok(());
        };
        let mut input = run.reader()?;
        let mut batch = Vec::new();
        let mut bytes = 0;
        while let Some(frame) = input.next(&self.key)? {
            let frame_bytes = frame.sealed.len();
            if batch.len() == MAX_APPLY_ENTRIES || bytes + frame_bytes > MAX_APPLY_BYTES {
                apply_batch(writer, &batch).await?;
                batch.clear();
                bytes = 0;
            }
            bytes += frame_bytes;
            batch.push(frame.entry);
        }
        apply_batch(writer, &batch).await
    }

    fn finish_run(&mut self) -> Result<Option<Run>> {
        let mut combined = None;
        for mut run in self.runs.drain(..).flatten() {
            combined = Some(match combined {
                Some(mut prior) => merge(&self.root, &self.key, &mut prior, &mut run)?,
                None => run,
            });
        }
        Ok(combined)
    }
}

async fn apply_batch(writer: &mut impl StagedReplayWriter, batch: &[MutationEntry]) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut keys = BTreeSet::new();
    let distinct = batch
        .iter()
        .filter(|entry| {
            let entry = *entry;
            keys.insert((&entry.schema, entry.operation.primary_key()))
        })
        .collect::<Vec<_>>();
    writer.apply_candidates(&distinct).await
}

struct Run {
    file: File,
    frames: u64,
    bytes: u64,
}

impl Run {
    fn reader(&mut self) -> Result<RunReader<'_>> {
        self.file.rewind()?;
        Ok(RunReader {
            file: BufReader::new(&mut self.file),
            remaining: self.frames,
        })
    }
}

struct RunReader<'a> {
    file: BufReader<&'a mut File>,
    remaining: u64,
}

struct Frame {
    entry: MutationEntry,
    sealed: Vec<u8>,
}

impl RunReader<'_> {
    fn next(&mut self, key: &[u8; 32]) -> Result<Option<Frame>> {
        if self.remaining == 0 {
            if !self.file.fill_buf()?.is_empty() {
                return Err(RestoreError::Invalid("trailing replay spool bytes"));
            }
            return Ok(None);
        }
        let mut length = [0; 4];
        self.file.read_exact(&mut length)?;
        let length = u32::from_be_bytes(length) as usize;
        if length == 0 || length > MAX_FRAME_BYTES {
            return Err(RestoreError::Invalid("replay spool frame length"));
        }
        let mut sealed = vec![0; length];
        self.file.read_exact(&mut sealed)?;
        let plaintext = Zeroizing::new(
            decrypt_envelope(key, &sealed)
                .map_err(|_| RestoreError::Invalid("spool authentication"))?,
        );
        let entry =
            MutationEntry::decode(&plaintext).map_err(|_| RestoreError::Invalid("spool entry"))?;
        self.remaining -= 1;
        Ok(Some(Frame { entry, sealed }))
    }
}

struct RunWriter {
    file: BufWriter<File>,
    frames: u64,
    bytes: u64,
}

impl RunWriter {
    fn new(root: &Path) -> Result<Self> {
        let path = root.join(format!(".replay-{}", uuid::Uuid::new_v4()));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        fs::remove_file(path)?;
        Ok(Self {
            file: BufWriter::new(file),
            frames: 0,
            bytes: 0,
        })
    }

    fn push(&mut self, sealed: &[u8]) -> Result<()> {
        if sealed.is_empty() || sealed.len() > MAX_FRAME_BYTES {
            return Err(RestoreError::Invalid("replay spool frame length"));
        }
        self.file.write_all(&(sealed.len() as u32).to_be_bytes())?;
        self.file.write_all(sealed)?;
        self.frames += 1;
        self.bytes += frame_bytes(sealed);
        Ok(())
    }

    fn finish(self) -> Result<Run> {
        Ok(Run {
            file: self
                .file
                .into_inner()
                .map_err(std::io::IntoInnerError::into_error)?,
            frames: self.frames,
            bytes: self.bytes,
        })
    }
}

fn frame_bytes(sealed: &[u8]) -> u64 {
    (sealed.len() + size_of::<u32>()) as u64
}

fn merge(root: &Path, key: &[u8; 32], left: &mut Run, right: &mut Run) -> Result<Run> {
    let mut output = RunWriter::new(root)?;
    let mut left = left.reader()?;
    let mut right = right.reader()?;
    let mut left_head = left.next(key)?;
    let mut right_head = right.next(key)?;
    loop {
        let take_left = match (&left_head, &right_head) {
            (Some(left), Some(right)) => match left.entry.stamp.cmp(&right.entry.stamp) {
                Ordering::Greater => true,
                Ordering::Less => false,
                Ordering::Equal => return Err(RestoreError::Invalid("duplicate replay position")),
            },
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return output.finish(),
        };
        let (head, source) = if take_left {
            (&mut left_head, &mut left)
        } else {
            (&mut right_head, &mut right)
        };
        let next = head
            .take()
            .ok_or(RestoreError::Invalid("replay spool merge"))?;
        output.push(&next.sealed)?;
        *head = source.next(key)?;
    }
}
