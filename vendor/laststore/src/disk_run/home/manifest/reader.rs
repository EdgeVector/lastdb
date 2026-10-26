//! The ordinary loader, extracted without changes to its codec or allocation policy.
use super::{corrupt, Manifest, MAX_CURRENT_BYTES};
use crate::frame::{decode_frame, min_encoded_len};
use crate::Result;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use uuid::Uuid;

impl Manifest {
    pub(in crate::disk_run::home) fn load_paths(
        current: &Path,
        runs: &Path,
        key: &[u8; 32],
        home_id: Uuid,
    ) -> Result<Self> {
        let mut file = File::open(current)?;
        let len = file.metadata()?.len();
        if len < min_encoded_len() as u64 || len > MAX_CURRENT_BYTES as u64 {
            return Err(corrupt("CURRENT length"));
        }
        let mut frame = Vec::with_capacity(len as usize);
        file.read_to_end(&mut frame)?;
        let decoded = decode_frame(key, &frame)?;
        let manifest = Self::decode(&decoded.payload, runs)?;
        if manifest.home_id != home_id {
            return Err(corrupt("CURRENT names another home"));
        }
        Ok(manifest)
    }
}
