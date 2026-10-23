//! Authenticate both private plaintext identity and public ciphertext address.

use super::FileBlobRef;
use crate::crypto::{CryptoProvider, LocalCryptoProvider};
use crate::sharing::file_blob_seal::{decode_hex_32, verify_file_blob_identity};
use crate::sync::error::{SyncError, SyncResult};

pub(super) async fn open_file_blob(
    reference: &FileBlobRef,
    ciphertext: &[u8],
) -> SyncResult<Vec<u8>> {
    if reference.encrypted_size_bytes != 0
        && reference.encrypted_size_bytes != ciphertext.len() as u64
    {
        return Err(SyncError::Crypto(
            "file blob ciphertext size mismatch".into(),
        ));
    }
    let dek = decode_hex_32(&reference.dek)?;
    let plaintext = LocalCryptoProvider::from_key(dek)
        .decrypt(ciphertext)
        .await?;
    verify_file_blob_identity(
        &reference.to_access(),
        &crate::hex::sha256_hex(&plaintext),
        &crate::hex::sha256_hex(ciphertext),
    )?;
    Ok(plaintext)
}
