use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{fs::File, io::Read, path::Path};

const LOCK: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../../Cargo.lock"));
const REPOSITORY_CACHE: &str = "models--Qdrant--all-MiniLM-L6-v2-onnx";
const FILES: [&str; 5] = [
    "model.onnx",
    "tokenizer.json",
    "config.json",
    "special_tokens_map.json",
    "tokenizer_config.json",
];

#[derive(Serialize)]
pub struct ModelFile {
    pub name: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Serialize)]
pub struct ModelEvidence {
    pub model_identity: String,
    pub fastembed_version: String,
    pub ort_version: String,
    pub cargo_lock_sha256: String,
    pub snapshot_revision: String,
    pub files: Vec<ModelFile>,
}

fn version(package: &str) -> Result<String, String> {
    let name = format!("name = \"{package}\"");
    LOCK.split("[[package]]")
        .find(|block| block.lines().any(|line| line == name))
        .and_then(|block| {
            block
                .lines()
                .find_map(|line| line.strip_prefix("version = \""))
        })
        .map(|value| value.trim_end_matches('"').to_owned())
        .ok_or_else(|| format!("{package} is absent from the compiled lockfile"))
}

fn model_file(root: &Path, name: &str) -> Result<ModelFile, String> {
    let mut file = File::open(root.join(name)).map_err(|error| error.to_string())?;
    let bytes = file.metadata().map_err(|error| error.to_string())?.len();
    let mut hash = Sha256::new();
    let mut buffer = [0; 16 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(ModelFile {
        name: name.into(),
        sha256: format!("{:x}", hash.finalize()),
        bytes,
    })
}

/// CI starts with an empty dedicated model cache. Refuse an ambiguous cache
/// rather than attribute measurements to an arbitrary model snapshot.
pub fn model_evidence(cache: &Path, identity: &str) -> Result<ModelEvidence, String> {
    let snapshots = cache.join(REPOSITORY_CACHE).join("snapshots");
    let entries = std::fs::read_dir(snapshots).map_err(|error| error.to_string())?;
    let mut snapshots = Vec::new();
    for entry in entries.take(17) {
        let path = entry.map_err(|error| error.to_string())?.path();
        if path.is_dir() {
            snapshots.push(path);
        }
    }
    if snapshots.len() != 1 {
        return Err("the dedicated cache must contain exactly one model snapshot".into());
    }
    let root = &snapshots[0];
    let files = FILES
        .iter()
        .map(|name| model_file(root, name))
        .collect::<Result<_, _>>()?;
    Ok(ModelEvidence {
        model_identity: identity.into(),
        fastembed_version: version("fastembed")?,
        ort_version: version("ort")?,
        cargo_lock_sha256: format!("{:x}", Sha256::digest(LOCK.as_bytes())),
        snapshot_revision: root
            .file_name()
            .ok_or("model snapshot has no revision")?
            .to_string_lossy()
            .into(),
        files,
    })
}
