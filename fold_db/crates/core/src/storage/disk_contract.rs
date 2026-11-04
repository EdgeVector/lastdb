//! Pin a frozen disk export. The hold paths stay in this module.
//!
//! Cloud planning receives descriptors and a read-only byte view. It does not
//! receive a path, a write handle, or a way to delete the source.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

use cloud_backup_protocol::{bytes_sha256, Digest};
use laststore::disk_run::home::{CapturedRunHome, RunHome};
use sha2::Digest as _;

use super::cloud_contract::{ArtifactDescriptor, Checkpoint, ContractError, EncodingContext};
use super::format::export::{classify, kind_name, part_by_id};
use super::format::{
    CAS_DIR, CAS_KEY_LABEL, HOME_FORMAT, HOME_KEY_LABEL, NODE_MARKER, RECORD_NODE_BYTES, RUN_DIR,
};

/// A read-only pin of one frozen node export.
pub struct PinnedDiskExport {
    main: CapturedRunHome,
    cas: CapturedRunHome,
    marker: PathBuf,
    descriptors: Vec<ArtifactDescriptor>,
}

impl PinnedDiskExport {
    pub fn descriptors(&self) -> &[ArtifactDescriptor] {
        &self.descriptors
    }

    /// Read pinned bytes. This opens the held file, not the live path.
    pub fn read_at(&self, identity: &str, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut file = self.open_held(identity)?;
        file.seek(SeekFrom::Start(offset))?;
        file.read(buf)
    }

    fn open_held(&self, identity: &str) -> std::io::Result<File> {
        if identity == NODE_MARKER {
            return File::open(&self.marker);
        }
        let Some((home_dir, relative)) = identity.split_once('/') else {
            return Err(missing(identity));
        };
        let capture = match home_dir {
            RUN_DIR => &self.main,
            CAS_DIR => &self.cas,
            _ => return Err(missing(identity)),
        };
        capture
            .reader(Path::new(relative))
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
}

fn missing(identity: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("unknown artifact {identity}"),
    )
}

struct Draft {
    identity: String,
    part_id: &'static str,
    role: super::cloud_contract::ExportRole,
    bytes: u64,
    sha256: Digest,
    encoding: EncodingContext,
    checkpoint: Checkpoint,
    depends: Vec<String>,
}

/// Capture both run homes and hardlink the node marker under `hold_dir`.
pub(crate) fn pin_homes(
    hold_dir: &Path,
    main: &RunHome,
    cas: &RunHome,
) -> Result<PinnedDiskExport, ContractError> {
    fs::DirBuilder::new()
        .mode(0o700)
        .create(hold_dir)
        .map_err(ContractError::Io)?;
    let main_hold = main
        .capture_files(&hold_dir.join("main"))
        .map_err(capture_error)?;
    let cas_hold = cas
        .capture_files(&hold_dir.join("cas"))
        .map_err(capture_error)?;
    let marker = link_marker(hold_dir, main)?;
    let checkpoint = Checkpoint {
        disk_format: HOME_FORMAT,
        main_home_id: main_hold.home_id.to_string(),
        main_generation: main_hold.generation,
        main_frontier: main_hold.frontier,
        cas_home_id: cas_hold.home_id.to_string(),
        cas_generation: cas_hold.generation,
        cas_frontier: cas_hold.frontier,
    };
    let mut drafts = Vec::new();
    mint_home(
        RUN_DIR,
        &main_hold,
        HOME_KEY_LABEL,
        &checkpoint,
        &mut drafts,
    )?;
    mint_home(CAS_DIR, &cas_hold, CAS_KEY_LABEL, &checkpoint, &mut drafts)?;
    drafts.push(mint_marker(&marker, &checkpoint, &main_hold)?);
    let expected = main_hold
        .files
        .len()
        .saturating_add(cas_hold.files.len())
        .saturating_add(1);
    if drafts.len() != expected {
        return Err(ContractError::capture(
            "pinned descriptor count differs from the frozen files",
        ));
    }
    resolve_dependencies(&mut drafts)?;
    reject_duplicate_identities(&drafts)?;
    drafts.sort_by(|left, right| left.identity.cmp(&right.identity));
    let descriptors = drafts.into_iter().map(descriptor).collect();
    Ok(PinnedDiskExport {
        main: main_hold,
        cas: cas_hold,
        marker,
        descriptors,
    })
}

fn link_marker(hold_dir: &Path, main: &RunHome) -> Result<PathBuf, ContractError> {
    let Some(node) = main.path().parent() else {
        return Err(ContractError::spec("run home has no node directory"));
    };
    let source = node.join(NODE_MARKER);
    let marker = hold_dir.join(NODE_MARKER);
    fs::hard_link(&source, &marker).map_err(ContractError::Io)?;
    Ok(marker)
}

fn mint_home(
    home_dir: &'static str,
    capture: &CapturedRunHome,
    key_label: &'static [u8],
    checkpoint: &Checkpoint,
    out: &mut Vec<Draft>,
) -> Result<(), ContractError> {
    let label = key_label_text(key_label)?;
    for file in &capture.files {
        let Some(part) = classify(home_dir, file.kind) else {
            return Err(ContractError::Unclassified(format!(
                "{home_dir} captured file"
            )));
        };
        let Some(relative) = file.path.to_str() else {
            return Err(ContractError::spec("captured path is not utf-8"));
        };
        let identity = format!("{home_dir}/{relative}");
        let mut reader = capture.reader(&file.path).map_err(capture_error)?;
        let (bytes, sha256) = hash_file(&mut reader)?;
        if bytes != file.bytes {
            return Err(ContractError::capture(
                "pinned length differs from the frozen file",
            ));
        }
        out.push(Draft {
            identity,
            part_id: part.id,
            role: part.role,
            bytes,
            sha256,
            encoding: EncodingContext {
                disk_format: HOME_FORMAT,
                home_dir,
                kind: kind_name(file.kind),
                key_label: label,
                home_id: capture.home_id.to_string(),
                generation: capture.generation,
                frontier: capture.frontier,
            },
            checkpoint: checkpoint.clone(),
            depends: Vec::new(),
        });
    }
    Ok(())
}

fn mint_marker(
    marker: &Path,
    checkpoint: &Checkpoint,
    main: &CapturedRunHome,
) -> Result<Draft, ContractError> {
    let mut file = File::open(marker).map_err(ContractError::Io)?;
    let (bytes, sha256) = hash_file(&mut file)?;
    if sha256 != bytes_sha256(RECORD_NODE_BYTES) {
        return Err(ContractError::capture(
            "node marker bytes differ from the disk format",
        ));
    }
    Ok(Draft {
        identity: NODE_MARKER.to_string(),
        part_id: "node-marker",
        role: super::cloud_contract::ExportRole::Snapshot,
        bytes,
        sha256,
        encoding: EncodingContext {
            disk_format: HOME_FORMAT,
            home_dir: "",
            kind: "node-marker",
            key_label: "",
            home_id: main.home_id.to_string(),
            generation: main.generation,
            frontier: main.frontier,
        },
        checkpoint: checkpoint.clone(),
        depends: Vec::new(),
    })
}

fn resolve_dependencies(drafts: &mut [Draft]) -> Result<(), ContractError> {
    let owners = owner_map(drafts);
    for draft in &mut *drafts {
        attach_dependencies(draft, &owners)?;
    }
    Ok(())
}

fn owner_map(drafts: &[Draft]) -> BTreeMap<&'static str, Vec<String>> {
    let mut owners: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    for draft in drafts {
        owners
            .entry(draft.part_id)
            .or_default()
            .push(draft.identity.clone());
    }
    owners
}

fn attach_dependencies(
    draft: &mut Draft,
    owners: &BTreeMap<&'static str, Vec<String>>,
) -> Result<(), ContractError> {
    let Some(part) = part_by_id(draft.part_id) else {
        return Err(ContractError::Unclassified(draft.part_id.to_string()));
    };
    for dependency in part.depends_on {
        let Some(ids) = owners.get(*dependency) else {
            let id = draft.part_id;
            return Err(ContractError::Incomplete(format!(
                "part {id} depends on {dependency}, which was not pinned"
            )));
        };
        let Some(identity) = one_identity(ids) else {
            return Err(ContractError::Incomplete(format!(
                "dependency {dependency} is not one artifact"
            )));
        };
        draft.depends.push(identity.to_string());
    }
    Ok(())
}

fn one_identity(ids: &[String]) -> Option<&str> {
    let [identity] = ids else {
        return None;
    };
    Some(identity.as_str())
}

fn reject_duplicate_identities(drafts: &[Draft]) -> Result<(), ContractError> {
    let mut seen = HashSet::new();
    for draft in drafts {
        if !seen.insert(draft.identity.as_str()) {
            let identity = &draft.identity;
            return Err(ContractError::spec(format!(
                "duplicate artifact identity {identity}"
            )));
        }
    }
    Ok(())
}

fn descriptor(draft: Draft) -> ArtifactDescriptor {
    ArtifactDescriptor {
        identity: draft.identity,
        part_id: draft.part_id,
        role: draft.role,
        bytes: draft.bytes,
        sha256: draft.sha256,
        encoding: draft.encoding,
        checkpoint: draft.checkpoint,
        depends_on: draft.depends,
        log_span: None,
    }
}

fn key_label_text(label: &'static [u8]) -> Result<&'static str, ContractError> {
    std::str::from_utf8(label).map_err(|_| ContractError::spec("run key label is not utf-8"))
}

fn hash_file(input: &mut impl Read) -> Result<(u64, Digest), ContractError> {
    let mut hasher = sha2::Sha256::new();
    let mut buf = [0_u8; 8192];
    let mut total = 0_u64;
    loop {
        let read = input.read(&mut buf).map_err(ContractError::Io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        let read = u64::try_from(read).map_err(|_| ContractError::spec("read length"))?;
        total = total
            .checked_add(read)
            .ok_or(ContractError::spec("artifact length overflow"))?;
    }
    if total == 0 {
        return Err(ContractError::spec("frozen artifact is empty"));
    }
    let result = hasher.finalize();
    let encoded: String = result.iter().map(|byte| format!("{byte:02x}")).collect();
    let digest = Digest::try_from(encoded).map_err(|_| ContractError::spec("sha256 encoding"))?;
    Ok((total, digest))
}

fn capture_error(error: laststore::Error) -> ContractError {
    ContractError::Capture(error.to_string())
}
