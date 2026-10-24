//! Authenticate exact existing ciphertext on disk without a file-sized allocation.
use super::*;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// The same open ciphertext handle used for length, digest and key proof.
/// No plaintext is exposed. A later copy must still check its exact final digest.
pub struct VerifiedRecoveredFile {
    file: File,
    stored_bytes: u64,
    stored_sha256: Digest,
}

impl VerifiedRecoveredFile {
    pub fn len(&self) -> u64 {
        self.stored_bytes
    }
    pub fn is_empty(&self) -> bool {
        self.stored_bytes == 0
    }
    pub fn stored_sha256(&self) -> &Digest {
        &self.stored_sha256
    }
}

impl Read for VerifiedRecoveredFile {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let maximum = output
            .len()
            .min(cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize);
        self.file.read(&mut output[..maximum])
    }
}
impl Seek for VerifiedRecoveredFile {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(position)
    }
}

/// A subsequent backup copies this original ciphertext from the restored home.
/// An absent exact map is a miss. Corruption never falls back to the old owner.
pub fn open_verified_ciphertext(
    data_root: &Path,
    home_id: &Id,
    pointer_id: &Digest,
    kind: FileDependencyKind,
    access: &FileBlobAccess,
) -> StorageResult<Option<VerifiedRecoveredFile>> {
    let Some(entry) = binding(data_root, home_id, pointer_id, kind, access)? else {
        return Ok(None);
    };
    let mut file = files::open_bytes(
        &object_path(data_root, &entry.stored_source_sha256),
        entry.stored_source_bytes.get(),
    )?;
    if verify_remote_ciphertext(access, &mut file)? != entry.stored_source_sha256 {
        return Err(invalid("recovered file stored identity"));
    }
    Ok(Some(VerifiedRecoveredFile {
        file,
        stored_bytes: entry.stored_source_bytes.get(),
        stored_sha256: entry.stored_source_sha256,
    }))
}

/// Validate the original remote envelope and pointer using bounded stream crypto.
/// The caller owns this private regular file and its disk admission. Success rewinds it.
pub fn verify_remote_ciphertext(access: &FileBlobAccess, file: &mut File) -> StorageResult<Digest> {
    let bytes = file.metadata()?.len();
    if !file.metadata()?.is_file()
        || !file_blob_cipher_suite_supported(&access.cipher_suite)
        || bytes < cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES
        || access.encrypted_size_bytes != Some(bytes)
    {
        return Err(invalid("recovered file envelope bounds"));
    }
    let key = zeroize::Zeroizing::new(
        crate::hex::hex_decode_array::<32>(&access.dek)
            .ok_or_else(|| invalid("recovered file key"))?,
    );
    file.rewind()?;
    let proof = crate::crypto::stream::verify_v1_stream(&key, file, bytes)
        .map_err(|_| invalid("recovered file authentication"))?;
    if proof
        .plaintext_bytes
        .checked_add(cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES)
        != Some(bytes)
        || file.metadata()?.len() != bytes
    {
        return Err(invalid("recovered file plaintext identity"));
    }
    crate::sharing::file_blob_seal::verify_file_blob_identity(
        access,
        &crate::hex::hex_lower(proof.plaintext_sha256),
        &crate::hex::hex_lower(proof.envelope_sha256),
    )
    .map_err(|_| invalid("recovered file identity"))?;
    file.rewind()?;
    Digest::try_from(crate::hex::hex_lower(proof.stored_sha256))
        .map_err(|_| invalid("recovered file stored identity"))
}
