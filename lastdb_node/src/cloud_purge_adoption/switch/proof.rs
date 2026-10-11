//! An immutable proof binds the data switch, sidecars, and retired queues.

use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Read;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct FileProof {
    pub name: PathBuf,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Serialize, Deserialize)]
pub(super) struct SwitchProof {
    pub format_version: u32,
    pub plan: AdoptionPlan,
    pub retained_sha256: String,
    pub retained_sidecars: Vec<FileProof>,
    pub installed_sidecars: Vec<FileProof>,
    pub installed_data_sha256: String,
    pub queue_detach_sha256: String,
}

fn path(home: &Path, plan: &AdoptionPlan) -> Result<PathBuf, String> {
    Ok(CloudPurgePeer::directory_for_home(home)
        .map_err(message)?
        .join(format!(
            "switch-proof-{}-{}-{}.json",
            fold_db::hex::sha256_hex(&serde_json::to_vec(&plan.account).map_err(message)?),
            plan.generation,
            plan.operation_id
        )))
}

pub(super) fn prepare(
    home: &Path,
    pending: &Pending,
    retained: &fold_db::sync::cloud_purge_peer::RetainedHome,
    candidate: &Path,
    sidecars: Vec<FileProof>,
    queues: String,
) -> Result<String, String> {
    if !fold_db::hex::is_lower_hex_sha256(&queues) {
        return Err("cloud adoption queue reset proof is absent".into());
    }
    let proof = SwitchProof {
        format_version: 1,
        plan: pending.plan.clone(),
        retained_sha256: retained.receipt_sha256.clone(),
        retained_sidecars: sidecars,
        installed_sidecars: sidecars_in(candidate)?,
        installed_data_sha256: data_sha256(&candidate.join("data"))?,
        queue_detach_sha256: queues,
    };
    crate::cloud_purge_runtime::atomic(&path(home, &pending.plan)?, &proof)?;
    Ok(fold_db::hex::sha256_hex(
        &serde_json::to_vec(&proof).map_err(message)?,
    ))
}

pub(super) fn load(home: &Path, pending: &Pending) -> Result<SwitchProof, String> {
    let encoded = fs::read(path(home, &pending.plan)?).map_err(message)?;
    if pending.switch_proof_sha256.as_deref() != Some(fold_db::hex::sha256_hex(&encoded).as_str()) {
        return Err("cloud adoption switch proof changed".into());
    }
    let proof: SwitchProof = serde_json::from_slice(&encoded).map_err(message)?;
    if proof.format_version != 1
        || !same_plan(&proof.plan, &pending.plan)
        || pending
            .retained
            .as_ref()
            .map(|value| value.receipt_sha256.as_str())
            != Some(proof.retained_sha256.as_str())
    {
        return Err("cloud adoption switch proof belongs to another replacement".into());
    }
    let retained = pending
        .retained
        .as_ref()
        .ok_or("cloud adoption retention absent")?;
    for file in &proof.retained_sidecars {
        verify_file(&retained.directory.join("sidecars"), file)?;
    }
    Ok(proof)
}

pub(super) fn verify_installed(home: &Path, proof: &SwitchProof) -> Result<(), String> {
    if data_sha256(&home.join("data"))? != proof.installed_data_sha256 {
        return Err("cloud adoption installed data differs from the prepared candidate".into());
    }
    for file in &proof.installed_sidecars {
        verify_file(home, file)?;
    }
    Ok(())
}

pub(super) fn sidecars_in(home: &Path) -> Result<Vec<FileProof>, String> {
    let mut files = Vec::new();
    for name in files::SIDECARS {
        let file = home.join(name);
        if file.exists() {
            files.push(file_proof(&file, PathBuf::from(name))?);
        }
    }
    Ok(files)
}

pub(super) fn verify_file(directory: &Path, expected: &FileProof) -> Result<(), String> {
    if file_proof(&directory.join(&expected.name), expected.name.clone())? != *expected {
        return Err("cloud adoption file differs from its durable proof".into());
    }
    Ok(())
}

fn file_proof(path: &Path, name: PathBuf) -> Result<FileProof, String> {
    let metadata = fs::symlink_metadata(path).map_err(message)?;
    if !metadata.file_type().is_file() {
        return Err("cloud adoption file has an unsupported type".into());
    }
    let mut file = File::open(path).map_err(message)?;
    let mut buffer = [0u8; 131072];
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    loop {
        let count = file.read(&mut buffer).map_err(message)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        bytes += count as u64;
    }
    if bytes != metadata.len() {
        return Err("cloud adoption file changed during its proof".into());
    }
    Ok(FileProof {
        name,
        bytes,
        sha256: fold_db::hex::hex_lower(digest.finalize()),
    })
}

pub(super) fn data_sha256(directory: &Path) -> Result<String, String> {
    let mut files = Vec::new();
    data_files(directory, directory, 0, &mut files)?;
    files.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(fold_db::hex::sha256_hex(
        &serde_json::to_vec(&files).map_err(message)?,
    ))
}

fn data_files(
    base: &Path,
    directory: &Path,
    depth: u32,
    files: &mut Vec<FileProof>,
) -> Result<(), String> {
    if depth > 64 {
        return Err("cloud adoption data directory exceeds the depth limit".into());
    }
    for entry in fs::read_dir(directory).map_err(message)? {
        let entry = entry.map_err(message)?;
        let kind = entry.file_type().map_err(message)?;
        if kind.is_dir() {
            data_files(base, &entry.path(), depth + 1, files)?;
        } else if kind.is_file() {
            files.push(file_proof(
                &entry.path(),
                entry
                    .path()
                    .strip_prefix(base)
                    .map_err(message)?
                    .to_path_buf(),
            )?);
        } else {
            return Err(
                "cloud adoption candidate contains a symbolic link or a runtime file".into(),
            );
        }
    }
    Ok(())
}
