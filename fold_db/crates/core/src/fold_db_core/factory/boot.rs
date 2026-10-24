//! Context for native boot decryption failures.

use crate::error::FoldDbError;

pub(super) fn is_decrypt_failure_text(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("aead")
        || lower.contains("aes-gcm")
        || lower.contains("decrypt")
        || lower.contains("wrong encryption key")
        || lower.contains("wrong master key")
        || lower.contains("encryption error")
        || lower.contains("key mismatch")
        || lower.contains("undecryptable")
}

pub(super) fn map_boot_decrypt_error(msg: String) -> FoldDbError {
    if is_decrypt_failure_text(&msg) {
        FoldDbError::Database(format!(
            "cannot decrypt existing store — wrong master key or incompatible build; \
             refusing to start fresh: {msg}"
        ))
    } else {
        FoldDbError::Config(msg)
    }
}
