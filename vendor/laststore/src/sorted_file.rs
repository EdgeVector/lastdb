//! Bounded physical files behind one immutable sorted-segment byte stream.
#![allow(dead_code)] // LastStore write-path wiring is a follow-up card.
//!
//! The manifest is published only after every part is durable. Readers open
//! only the part needed by a seek, with at most one part descriptor at a time.
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub(crate) const PART_BYTES: u64 = 8 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"LSPART1\0";
pub(crate) const MANIFEST_BYTES: usize = 40;
const PIECE_MAGIC: &[u8; 8] = b"LSPIECE1";
const PIECE_HEADER_BYTES: usize = 40;
const PAYLOAD_BYTES: u64 = PART_BYTES - PIECE_HEADER_BYTES as u64;
const MAX_PARTS: u64 = 65_536;

#[derive(Clone, Copy, Debug)]
struct Piece {
    uuid: Uuid,
    index: u32,
    length: u32,
    crc: u32,
}

impl Piece {
    fn bytes(self) -> [u8; PIECE_HEADER_BYTES] {
        let mut bytes = [0; PIECE_HEADER_BYTES];
        bytes[..8].copy_from_slice(PIECE_MAGIC);
        bytes[8..24].copy_from_slice(self.uuid.as_bytes());
        bytes[24..28].copy_from_slice(&self.index.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.length.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.crc.to_le_bytes());
        let crc = crc32fast::hash(&bytes[..36]);
        bytes[36..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    fn read(file: &mut impl Read) -> io::Result<Self> {
        let mut bytes = [0; PIECE_HEADER_BYTES];
        file.read_exact(&mut bytes)?;
        if &bytes[..8] != PIECE_MAGIC
            || crc32fast::hash(&bytes[..36]) != u32::from_le_bytes(bytes[36..].try_into().unwrap())
        {
            return Err(invalid("invalid sorted piece header"));
        }
        let piece = Self {
            uuid: Uuid::from_slice(&bytes[8..24]).map_err(|_| invalid("invalid piece UUID"))?,
            index: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
            length: u32::from_le_bytes(bytes[28..32].try_into().unwrap()),
            crc: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
        };
        if piece.length == 0
            || piece.length as u64 > PAYLOAD_BYTES
            || piece.index as u64 >= MAX_PARTS
        {
            return Err(invalid("invalid sorted piece extent"));
        }
        Ok(piece)
    }

    fn backup_uuid(self) -> Uuid {
        let mut hash = Sha256::new();
        hash.update(b"laststore/sorted-piece/v1\0");
        hash.update(self.uuid.as_bytes());
        hash.update(self.index.to_le_bytes());
        Uuid::from_bytes(hash.finalize()[..16].try_into().unwrap())
    }
}

pub(crate) fn is_manifest(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}
pub(crate) fn is_piece(bytes: &[u8]) -> bool {
    bytes.starts_with(PIECE_MAGIC)
}

/// Only a committed manifest enumerates parts. Abandoned generation folders
/// never enter a backup, even when a process died before its Drop cleanup.
pub(crate) fn physical_parts(path: &Path) -> io::Result<Vec<(Uuid, PathBuf)>> {
    let input = Input::open(path)?;
    let Some((home, uuid)) = input.parts else {
        return Ok(Vec::new());
    };
    Ok((0..input.length.div_ceil(PAYLOAD_BYTES) as u32)
        .map(|index| {
            let piece = Piece {
                uuid,
                index,
                length: 0,
                crc: 0,
            };
            (piece.backup_uuid(), part_path(&home, index))
        })
        .collect())
}

pub(crate) fn verify_piece(path: &Path, expected_uuid: Uuid) -> io::Result<()> {
    let mut file = File::open(path)?;
    let piece = Piece::read(&mut file)?;
    if piece.backup_uuid() != expected_uuid
        || file.metadata()?.len() != piece.length as u64 + PIECE_HEADER_BYTES as u64
    {
        return Err(invalid("sorted backup piece identity or length differs"));
    }
    let mut hash = crc32fast::Hasher::new();
    let mut scratch = [0; 4096];
    loop {
        let count = file.read(&mut scratch)?;
        if count == 0 {
            break;
        }
        hash.update(&scratch[..count]);
    }
    if hash.finalize() != piece.crc {
        return Err(invalid("sorted backup piece checksum differs"));
    }
    Ok(())
}

pub(crate) fn install_piece(
    group: &Path,
    expected_uuid: Uuid,
    bytes: &[u8],
) -> io::Result<PathBuf> {
    if bytes.len() as u64 > PART_BYTES {
        return Err(invalid("sorted backup piece exceeds physical cap"));
    }
    let piece = Piece::read(&mut io::Cursor::new(bytes))?;
    if piece.backup_uuid() != expected_uuid
        || bytes.len() != PIECE_HEADER_BYTES + piece.length as usize
        || crc32fast::hash(&bytes[PIECE_HEADER_BYTES..]) != piece.crc
    {
        return Err(invalid("invalid sorted backup piece"));
    }
    let home = parts_home(&group.join("unused.seg"), piece.uuid)?;
    fs::create_dir_all(&home)?;
    let destination = part_path(&home, piece.index);
    if destination.exists() {
        if fs::metadata(&destination)?.len() != bytes.len() as u64
            || fs::read(&destination)? != bytes
        {
            return Err(invalid("restore would replace an immutable sorted piece"));
        }
        return Ok(destination);
    }
    let temporary = destination.with_extension(format!("{}.installing", Uuid::new_v4()));
    let result = (|| {
        let mut file = create(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::hard_link(&temporary, &destination)?;
        fs::remove_file(&temporary)?;
        File::open(&home)?.sync_all()?;
        File::open(parent(&home)?)?.sync_all()?;
        File::open(group)?.sync_all()?;
        Ok(destination)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

/// A restored manifest stays unpublished until every named piece is present.
/// This check reads headers only; Segment::verify performs the full proof.
pub(crate) fn parts_present(path: &Path) -> io::Result<bool> {
    let input = Input::open(path)?;
    let Some((home, uuid)) = input.parts else {
        return Ok(true);
    };
    for index in 0..input.length.div_ceil(PAYLOAD_BYTES) as u32 {
        let mut file = match File::open(part_path(&home, index)) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        let piece = Piece::read(&mut file)?;
        let expected = (input.length - index as u64 * PAYLOAD_BYTES).min(PAYLOAD_BYTES);
        if piece.uuid != uuid
            || piece.index != index
            || piece.length as u64 != expected
            || file.metadata()?.len() != expected + PIECE_HEADER_BYTES as u64
        {
            return Err(invalid("restored sorted piece differs from its manifest"));
        }
    }
    Ok(true)
}

pub(crate) fn remove(path: &Path) -> io::Result<()> {
    let input = Input::open(path)?;
    fs::remove_file(path)?;
    if let Some((home, _)) = input.parts {
        // Commit removal of the manifest before its dependencies disappear.
        File::open(parent(path)?)?.sync_all()?;
        match fs::remove_dir_all(&home) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        File::open(parent(&home)?)?.sync_all()?;
    }
    Ok(())
}

/// Reap crash remnants under one locked group after a successful full merge.
/// Every live or pending manifest is checked before any directory is removed.
pub(crate) fn cleanup_orphans(group: &Path) -> io::Result<()> {
    let root = group.join(".sorted-parts");
    let mut active = std::collections::HashSet::new();
    let mut staged = Vec::new();
    for entry in fs::read_dir(group)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some((sequence, uuid)) = name
            .strip_suffix(".sorted-stage")
            .and_then(|name| name.split_once('.'))
        {
            if sequence.parse::<u64>().is_ok() && Uuid::parse_str(uuid).is_ok() {
                staged.push(entry.path());
            }
            continue;
        }
        if !(name.ends_with(".seg") || name.ends_with(".seg.installing")) {
            continue;
        }
        if let Some(uuid) = Input::open(&entry.path())?.uuid() {
            active.insert(uuid);
        }
    }
    // Staging links never make a generation visible. Remove only the link:
    // its parts may also belong to a committed numeric manifest after a crash.
    for path in staged {
        fs::remove_file(path)?;
    }
    // The full-merge caller syncs the group after this cleanup returns.
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let Ok(uuid) = Uuid::parse_str(name.strip_suffix(".tmp").unwrap_or(&name)) else {
            continue;
        };
        if !active.contains(&uuid) {
            fs::remove_dir_all(entry.path())?;
        }
    }
    File::open(root)?.sync_all()
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn parent(path: &Path) -> io::Result<&Path> {
    path.parent()
        .ok_or_else(|| invalid("sorted file has no parent"))
}

fn parts_home(path: &Path, uuid: Uuid) -> io::Result<PathBuf> {
    Ok(parent(path)?.join(".sorted-parts").join(uuid.to_string()))
}

fn part_path(home: &Path, index: u32) -> PathBuf {
    home.join(format!("{index:08x}.part"))
}

fn create(path: &Path) -> io::Result<File> {
    OpenOptions::new().create_new(true).write(true).open(path)
}

pub(crate) struct Output {
    destination: PathBuf,
    temporary: PathBuf,
    uuid: Uuid,
    file: Option<File>,
    position: u64,
    part: u32,
    staging: Option<PathBuf>,
    published_parts: bool,
    published: bool,
    crc: crc32fast::Hasher,
}

impl Output {
    pub(crate) fn new(path: &Path, uuid: Uuid) -> io::Result<Self> {
        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "sorted destination exists",
            ));
        }
        let temporary = path.with_extension(format!("{uuid}.tmp"));
        let file = create(&temporary)?;
        Ok(Self {
            destination: path.into(),
            temporary,
            uuid,
            file: Some(file),
            position: 0,
            part: 0,
            staging: None,
            published_parts: false,
            published: false,
            crc: crc32fast::Hasher::new(),
        })
    }

    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    fn finish_part(&mut self) -> io::Result<()> {
        let piece = self.piece();
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| invalid("closed sorted output"))?;
        if self.staging.is_some() {
            file.rewind()?;
            file.write_all(&piece.bytes())?;
        }
        file.sync_all()
    }

    fn piece(&self) -> Piece {
        Piece {
            uuid: self.uuid,
            index: self.part,
            length: (self.position - self.part as u64 * PAYLOAD_BYTES) as u32,
            crc: self.crc.clone().finalize(),
        }
    }

    fn roll(&mut self) -> io::Result<()> {
        let next = self
            .part
            .checked_add(1)
            .ok_or_else(|| invalid("too many sorted parts"))?;
        if next as u64 >= MAX_PARTS {
            return Err(invalid("too many sorted parts"));
        }
        self.finish_part()?;
        self.file.take();
        if self.staging.is_none() {
            let final_home = parts_home(&self.destination, self.uuid)?;
            fs::create_dir_all(parent(&final_home)?)?;
            let staging = final_home.with_extension("tmp");
            fs::create_dir(&staging)?;
            self.staging = Some(staging.clone());
            // The first piece was a regular file until the stream exceeded a
            // part. Add its transport header with one bounded streaming copy.
            let mut first = create(&part_path(&staging, 0))?;
            first.write_all(&self.piece().bytes())?;
            let copied = io::copy(&mut File::open(&self.temporary)?, &mut first)?;
            if copied != PAYLOAD_BYTES {
                return Err(invalid("short first sorted piece"));
            }
            first.sync_all()?;
            fs::remove_file(&self.temporary)?;
        }
        let mut file = create(&part_path(self.staging.as_ref().unwrap(), next))?;
        file.write_all(&[0; PIECE_HEADER_BYTES])?;
        self.file = Some(file);
        self.part = next;
        self.crc = crc32fast::Hasher::new();
        Ok(())
    }

    pub(crate) fn publish(mut self) -> io::Result<()> {
        self.finish_part()?;
        self.file.take();
        if let Some(staging) = &self.staging {
            File::open(staging)?.sync_all()?;
            let final_home = parts_home(&self.destination, self.uuid)?;
            // UUID directories are never reused; no existing generation is replaced.
            if final_home.exists() {
                return Err(invalid("sorted generation already exists"));
            }
            fs::rename(staging, &final_home)?;
            self.published_parts = true;
            File::open(parent(&final_home)?)?.sync_all()?;
            File::open(parent(parent(&final_home)?)?)?.sync_all()?;
            let mut manifest = [0; MANIFEST_BYTES];
            manifest[..8].copy_from_slice(MAGIC);
            manifest[8..24].copy_from_slice(self.uuid.as_bytes());
            manifest[24..32].copy_from_slice(&self.position.to_le_bytes());
            let crc = crc32fast::hash(&manifest[..32]);
            manifest[32..36].copy_from_slice(&crc.to_le_bytes());
            let mut file = create(&self.temporary)?;
            file.write_all(&manifest)?;
            file.sync_all()?;
        }
        // hard_link is an atomic, no-replace publication in this directory.
        // A competing writer cannot have its committed destination overwritten.
        fs::hard_link(&self.temporary, &self.destination)?;
        self.published = true;
        fs::remove_file(&self.temporary)?;
        File::open(parent(&self.destination)?)?.sync_all()
    }
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.position / PAYLOAD_BYTES > self.part as u64 {
            self.roll()?;
        }
        let limit = bytes
            .len()
            .min((PAYLOAD_BYTES - self.position % PAYLOAD_BYTES) as usize);
        let written = self
            .file
            .as_mut()
            .ok_or_else(|| invalid("closed sorted output"))?
            .write(&bytes[..limit])?;
        self.crc.update(&bytes[..written]);
        self.position = self
            .position
            .checked_add(written as u64)
            .ok_or_else(|| invalid("sorted length overflow"))?;
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file
            .as_mut()
            .ok_or_else(|| invalid("closed sorted output"))?
            .flush()
    }
}

impl Drop for Output {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.temporary);
        if !self.published {
            if let Some(staging) = &self.staging {
                // One owned generation only, never the store or workspace.
                let _ = fs::remove_dir_all(staging);
            }
            if self.published_parts {
                if let Ok(home) = parts_home(&self.destination, self.uuid) {
                    let _ = fs::remove_dir_all(home);
                }
            }
        }
    }
}

pub(crate) struct Input {
    length: u64,
    position: u64,
    parts: Option<(PathBuf, Uuid)>,
    file: Option<(u32, File)>,
}

impl Input {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let physical_len = file.metadata()?.len();
        let mut magic = [0; 8];
        if physical_len < 8 {
            return Ok(Self {
                length: physical_len,
                position: 0,
                parts: None,
                file: Some((0, file)),
            });
        }
        file.read_exact(&mut magic)?;
        if &magic != MAGIC {
            file.rewind()?;
            return Ok(Self {
                length: physical_len,
                position: 0,
                parts: None,
                file: Some((0, file)),
            });
        }
        if physical_len != MANIFEST_BYTES as u64 {
            return Err(invalid("invalid sorted manifest size"));
        }
        file.rewind()?;
        let mut manifest = [0; MANIFEST_BYTES];
        file.read_exact(&mut manifest)?;
        if manifest[36..] != [0; 4]
            || crc32fast::hash(&manifest[..32])
                != u32::from_le_bytes(manifest[32..36].try_into().unwrap())
        {
            return Err(invalid("invalid sorted manifest checksum"));
        }
        let uuid = Uuid::from_slice(&manifest[8..24])
            .map_err(|_| invalid("invalid sorted manifest UUID"))?;
        let length = u64::from_le_bytes(manifest[24..32].try_into().unwrap());
        if length <= PAYLOAD_BYTES || length.div_ceil(PAYLOAD_BYTES) > MAX_PARTS {
            return Err(invalid("invalid sorted manifest length"));
        }
        Ok(Self {
            length,
            position: 0,
            parts: Some((parts_home(path, uuid)?, uuid)),
            file: None,
        })
    }
    pub(crate) fn len(&self) -> u64 {
        self.length
    }
    pub(crate) fn uuid(&self) -> Option<Uuid> {
        self.parts.as_ref().map(|(_, uuid)| *uuid)
    }
}

impl Read for Input {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.length || bytes.is_empty() {
            return Ok(0);
        }
        let index = if self.parts.is_some() {
            (self.position / PAYLOAD_BYTES) as u32
        } else {
            0
        };
        if let Some((home, uuid)) = &self.parts {
            if self
                .file
                .as_ref()
                .map_or(true, |(current, _)| *current != index)
            {
                let mut file = File::open(part_path(home, index))?;
                let expected = (self.length - index as u64 * PAYLOAD_BYTES).min(PAYLOAD_BYTES);
                let piece = Piece::read(&mut file)?;
                if piece.uuid != *uuid
                    || piece.index != index
                    || piece.length as u64 != expected
                    || file.metadata()?.len() != expected + PIECE_HEADER_BYTES as u64
                {
                    return Err(invalid("sorted part has wrong length"));
                }
                self.file = Some((index, file));
            }
        }
        let remaining = self.length - self.position;
        let offset = if self.parts.is_some() {
            self.position % PAYLOAD_BYTES
        } else {
            self.position
        };
        let limit = if self.parts.is_some() {
            remaining.min(PAYLOAD_BYTES - offset)
        } else {
            remaining
        };
        let file = &mut self
            .file
            .as_mut()
            .ok_or_else(|| invalid("sorted input has no file"))?
            .1;
        file.seek(SeekFrom::Start(
            offset
                + if self.parts.is_some() {
                    PIECE_HEADER_BYTES as u64
                } else {
                    0
                },
        ))?;
        let limit = bytes.len().min(limit as usize);
        let count = file.read(&mut bytes[..limit])?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short sorted part",
            ));
        }
        self.position += count as u64;
        Ok(count)
    }
}

impl Seek for Input {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(position) => position as i128,
            SeekFrom::End(delta) => self.length as i128 + delta as i128,
            SeekFrom::Current(delta) => self.position as i128 + delta as i128,
        };
        self.position = u64::try_from(position).map_err(|_| invalid("invalid sorted seek"))?;
        Ok(self.position)
    }
}
