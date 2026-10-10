//! Bounded input reads and streamed private physical-copy evidence.

use super::*;
use serde::Serialize;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

const COPIES_FILE: &str = "selected-file-blob-copies.jsonl";

#[derive(PartialEq, Eq)]
pub(super) struct Input {
    pub refs: BTreeSet<String>,
    pub sha256: String,
}

pub(super) fn read_input(args: &FileBlobGcArgs) -> Result<Input, String> {
    let path = args
        .selected_blob_refs_file
        .as_ref()
        .ok_or("selected blob input is absent")?;
    let expected = args
        .selected_blob_refs_sha256
        .as_ref()
        .ok_or("selected blob input SHA is absent")?;
    super::super::pointers::valid_blob_ref(&format!("sha256:{expected}"))?;
    let metadata = std::fs::symlink_metadata(path).map_err(err)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err("selected blob input must be a regular private file".into());
    }
    let mut reader = BufReader::new(File::open(path).map_err(err)?);
    let mut hash = Sha256::new();
    let mut refs = BTreeSet::new();
    loop {
        let mut line = String::new();
        if (&mut reader).take(73).read_line(&mut line).map_err(err)? == 0 {
            break;
        }
        hash.update(line.as_bytes());
        let reference = line.trim_end_matches(['\r', '\n']);
        super::super::pointers::valid_blob_ref(reference)?;
        if !refs.insert(reference.to_string()) {
            return Err("selected blob input contains a duplicate reference".into());
        }
    }
    let sha256 = fold_db::hex::hex_lower(hash.finalize());
    if refs.is_empty() || &sha256 != expected {
        return Err("selected blob input is empty or its SHA differs".into());
    }
    Ok(Input { refs, sha256 })
}

#[derive(Default, Serialize)]
pub(super) struct Counts {
    pub physical_rows: u64,
    pub supported_blob_rows: u64,
    pub selected_copies: u64,
    pub selected_raw_bytes: u64,
    pub selected_scoped_copies: u64,
    pub selected_undated_copies: u64,
}

#[derive(Serialize)]
pub(super) struct Copy {
    pub collection: String,
    pub shard: u16,
    pub group_id: Option<u32>,
    pub scope: String,
    pub key_b64: String,
    pub blob_ref: String,
    pub raw_sha256: String,
    pub raw_bytes: u64,
    pub plain_sha256: String,
    pub stored_at: Option<String>,
}

#[derive(Serialize)]
pub(super) struct CopyFile {
    pub filename: &'static str,
    pub sha256: String,
    pub bytes: u64,
    pub records: u64,
}

pub(super) struct CopyWriter {
    writer: BufWriter<File>,
    hash: Sha256,
    bytes: u64,
    records: u64,
}

impl CopyWriter {
    fn new(dir: &Path) -> Result<Self, String> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(dir.join(COPIES_FILE))
            .map_err(err)?;
        Ok(Self {
            writer: BufWriter::with_capacity(65536, file),
            hash: Sha256::new(),
            bytes: 0,
            records: 0,
        })
    }

    pub(super) fn write(&mut self, copy: &Copy) -> Result<(), String> {
        let mut bytes = serde_json::to_vec(copy).map_err(err)?;
        bytes.push(b'\n');
        self.writer.write_all(&bytes).map_err(err)?;
        self.hash.update(&bytes);
        self.bytes += bytes.len() as u64;
        self.records += 1;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<CopyFile, String> {
        self.writer.flush().map_err(err)?;
        self.writer.get_ref().sync_all().map_err(err)?;
        Ok(CopyFile {
            filename: COPIES_FILE,
            sha256: fold_db::hex::hex_lower(self.hash.finalize()),
            bytes: self.bytes,
            records: self.records,
        })
    }
}

pub(super) struct Collected {
    pub found: BTreeSet<String>,
    pub keys: HashSet<(String, Vec<u8>)>,
    pub digests: BTreeMap<String, String>,
    pub counts: Counts,
    pub copies: CopyWriter,
}

impl Collected {
    pub(super) fn new(dir: &Path) -> Result<Self, String> {
        Ok(Self {
            found: BTreeSet::new(),
            keys: HashSet::new(),
            digests: BTreeMap::new(),
            counts: Counts::default(),
            copies: CopyWriter::new(dir)?,
        })
    }
}

#[derive(Serialize)]
pub(super) struct Inventory {
    pub format: u32,
    pub home: PathBuf,
    pub store_root: PathBuf,
    pub created_at: String,
    pub input_file_sha256: String,
    pub requested_refs: BTreeSet<String>,
    pub found_refs: BTreeSet<String>,
    pub absent_refs: BTreeSet<String>,
    pub namespace_digests: BTreeMap<String, String>,
    pub counts: Counts,
    pub copies: CopyFile,
    pub retirement_state_sha256: Option<String>,
    pub complete: bool,
    pub read_only: bool,
    pub delete_authority: bool,
    pub reference_holds_checked: bool,
    pub remote_absence_proved: bool,
    pub scope_note: &'static str,
}

#[derive(Serialize)]
pub(super) struct PublicReport<'a> {
    event: &'static str,
    complete: bool,
    read_only: bool,
    delete_authority: bool,
    reference_holds_checked: bool,
    remote_absence_proved: bool,
    requested_refs: u64,
    found_refs: u64,
    absent_refs: u64,
    physical_namespaces: u64,
    counts: &'a Counts,
}

impl<'a> From<&'a Inventory> for PublicReport<'a> {
    fn from(report: &'a Inventory) -> Self {
        Self {
            event: "selected_file_blob_inventory",
            complete: report.complete,
            read_only: true,
            delete_authority: false,
            reference_holds_checked: false,
            remote_absence_proved: false,
            requested_refs: report.requested_refs.len() as u64,
            found_refs: report.found_refs.len() as u64,
            absent_refs: report.absent_refs.len() as u64,
            physical_namespaces: report.namespace_digests.len() as u64,
            counts: &report.counts,
        }
    }
}
