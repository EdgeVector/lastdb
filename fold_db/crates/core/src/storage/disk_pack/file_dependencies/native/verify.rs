use super::{invalid, scratch, FileDependency, NativeCasProof, SpacePolicy, StorageResult};
use crate::crypto::stream::open_at_rest_stream;
use std::io::{self, BufReader, BufWriter, Read, Seek};
use std::path::Path;

pub(super) fn verify(
    proof: NativeCasProof,
    raw_path: &Path,
    account_key: &[u8; 32],
    dependency: &FileDependency,
    policy: &SpacePolicy,
) -> StorageResult<NativeCasProof> {
    let json_path = raw_path.with_extension("json.pending");
    let inner_path = raw_path.with_extension("inner.pending");
    scratch::remove(&[&json_path, &inner_path])?;
    let result = (|| {
        let mut raw = scratch::input(raw_path)?;
        if proof.collection != "cas_blobs"
            || proof.key
                != crate::storage::disk_pack::native_routing::encode(
                    dependency.blob_ref().as_bytes(),
                )
            || raw.metadata()?.len() != proof.stored_row_bytes
        {
            return Err(invalid("native CAS raw source identity"));
        }
        let mut prefix = [0; 4];
        raw.read_exact(&mut prefix)?;
        raw.rewind()?;
        if &prefix != b"ENB:" {
            return Err(invalid("native CAS requires its ENB account envelope"));
        }
        let account_v2 =
            dependency.encryption_suite() == crate::sharing::delivery_wire::FILE_BLOB_CIPHER_SUITE;
        let mut json = scratch::Scratch::create(&json_path, policy)?;
        let opened = open_at_rest_stream(account_key, &mut raw, &mut json)
            .map_err(|_| invalid("native CAS account envelope"))?;
        if crate::hex::hex_lower(opened.stored_sha256) != proof.stored_row_sha256.as_str() {
            return Err(invalid("native CAS raw source digest"));
        }
        let json = json.finish()?;
        let buffer = lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize;
        let mut inner =
            BufWriter::with_capacity(buffer, scratch::Scratch::create(&inner_path, policy)?);
        let size = super::json::extract(
            BufReader::with_capacity(buffer, json),
            &mut inner,
            dependency,
        )?;
        let mut inner = inner
            .into_inner()
            .map_err(|_| invalid("native CAS inner output"))?
            .finish()?;
        if account_v2 {
            inner.read_exact(&mut prefix)?;
            inner.rewind()?;
            if &prefix != b"ENC:" {
                return Err(invalid("account-v2 native CAS requires ENC text"));
            }
        }
        let key = zeroize::Zeroizing::new(
            crate::hex::hex_decode_array::<32>(&dependency.access().dek)
                .ok_or_else(|| invalid("file dependency key format"))?,
        );
        let verified = open_at_rest_stream(&key, &mut inner, &mut io::sink())
            .map_err(|_| invalid("native CAS per-file envelope"))?;
        if verified.plaintext_bytes != size {
            return Err(invalid("native CAS plaintext identity"));
        }
        crate::sharing::file_blob_seal::verify_file_blob_identity(
            dependency.access(),
            &crate::hex::hex_lower(verified.plaintext_sha256),
            &crate::hex::hex_lower(verified.envelope_sha256),
        )
        .map_err(|_| invalid("native CAS file identity"))?;
        Ok(proof)
    })();
    let cleanup = scratch::remove(&[&json_path, &inner_path]);
    match result {
        Ok(proof) => {
            cleanup?;
            Ok(proof)
        }
        Err(error) => {
            cleanup?;
            Err(error)
        }
    }
}
