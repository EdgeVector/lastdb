//! Client-only mutation-log manifests. The cloud never decodes these values.
mod page;
mod root;
use cloud_backup_protocol::v2::{AcceptedBackup, Begin, Destination};
use cloud_backup_protocol::{Digest, RecoveryDescriptor};
use fold_db::storage::disk_pack::byte_backup::{
    ClientNativeFile, ClientPageReference, CODEC_ENVELOPE_MAX_BYTES, CODEC_PLAINTEXT_MAX_BYTES,
    MAX_CLIENT_PAGES, MAX_PAGE_FILES,
};
use fold_db::storage::disk_pack::mutation_log::{log_path, LogAnchor, LogEntry, LogPosition};
pub use page::MutationLogPage;
pub use root::{LogKind, MutationLogRoot};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
pub const VERSION: u16 = 4;
pub const MAX_BATCH_ENTRIES: usize = 128;

#[derive(Debug)]
pub struct LogCodecError(&'static str);
impl std::fmt::Display for LogCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for LogCodecError {}
pub type Result<T> = std::result::Result<T, LogCodecError>;

fn context(version: u16, digest: &Digest, descriptor: &Digest, begin: &Begin) -> Result<()> {
    begin
        .validate()
        .map_err(|_| LogCodecError("mutation log begin"))?;
    if version != VERSION
        || !begin.descriptor.as_value().is_null()
        || begin.required_destinations != [Destination::R2, Destination::B2]
        || *digest
            != begin
                .input_sha256()
                .map_err(|_| LogCodecError("mutation log begin digest"))?
        || *descriptor
            != begin
                .descriptor
                .commitment()
                .map_err(|_| LogCodecError("mutation log descriptor digest"))?
    {
        return Err(LogCodecError("mutation log metadata frozen context"));
    }
    Ok(())
}

pub fn encode(value: &impl Serialize, key: &[u8; 32]) -> Result<Vec<u8>> {
    let bytes = zeroize::Zeroizing::new(
        serde_json::to_vec(value).map_err(|_| LogCodecError("mutation log metadata encoding"))?,
    );
    if bytes.len() as u64 > CODEC_PLAINTEXT_MAX_BYTES {
        return Err(LogCodecError("mutation log metadata size"));
    }
    fold_db::crypto::encrypt_envelope(key, &bytes)
        .map_err(|_| LogCodecError("mutation log metadata protection"))
}

fn decode<T: DeserializeOwned>(bytes: &[u8], key: &[u8; 32]) -> Result<T> {
    if bytes.len() as u64 > CODEC_ENVELOPE_MAX_BYTES {
        return Err(LogCodecError("mutation log metadata envelope size"));
    }
    let plain = zeroize::Zeroizing::new(
        fold_db::crypto::decrypt_envelope(key, bytes)
            .map_err(|_| LogCodecError("mutation log metadata protection"))?,
    );
    if plain.len() as u64 > CODEC_PLAINTEXT_MAX_BYTES {
        return Err(LogCodecError("mutation log metadata size"));
    }
    serde_json::from_slice(&plain).map_err(|_| LogCodecError("mutation log metadata format"))
}

fn encoded_bound(value: &impl Serialize) -> Result<()> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| LogCodecError("mutation log metadata encoding"))?;
    if bytes.len() as u64 > CODEC_PLAINTEXT_MAX_BYTES {
        return Err(LogCodecError("mutation log metadata size"));
    }
    Ok(())
}
