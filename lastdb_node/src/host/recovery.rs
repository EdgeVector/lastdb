//! Boot recovery-phrase emission, Spotlight opt-out, and `last_boot_error.txt` persistence.

use std::io::{self, IsTerminal};
use std::path::Path;

/// File under the node home recording the last boot failure message.
pub const LAST_BOOT_ERROR_FILE: &str = "last_boot_error.txt";

pub(super) fn emit_fresh_recovery_phrase(mnemonic: &str) {
    if std::env::var_os("CI").is_none()
        && std::env::var_os("LASTDB_SUPPRESS_RECOVERY_PHRASE").is_none()
        && io::stderr().is_terminal()
    {
        eprint!(
            "\n=== LastDB recovery phrase (save this; only shown once) ===\n\
             {mnemonic}\n\
             ===========================================================\n\
             This phrase is your account root: identity, content encryption, \
             and HashKey blinding. Enabling cloud later reuses it — \
             no re-encryption of field keys.\n"
        );
        tracing::info!("fresh identity generated; recovery phrase printed on stderr");
    } else {
        eprintln!("=== LastDB recovery phrase generated (redacted; not printed in CI/non-TTY) ===");
        tracing::info!("fresh identity generated; recovery phrase redacted (CI/non-TTY)");
    }
}

pub(super) fn ensure_node_home(home: &Path) -> io::Result<()> {
    std::fs::create_dir_all(home)?;
    #[cfg(target_os = "macos")]
    {
        let marker = home.join(".metadata_never_index");
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker)
        {
            Ok(_) => {
                tracing::info!(path = %marker.display(), "excluded node home from Spotlight indexing");
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => tracing::warn!(
                target: "lastdb_node::host", path = %marker.display(), error = %e,
                "could not exclude node home from Spotlight indexing"
            ),
        }
    }
    Ok(())
}

pub(super) fn map_and_record_boot_error(
    home: &Path,
    existing_store: bool,
    err: impl std::fmt::Display,
) -> String {
    let raw = err.to_string();
    let lower = raw.to_ascii_lowercase();
    let decrypt_err = [
        "cannot decrypt existing store",
        "aead",
        "aes-gcm",
        "decrypt",
        "wrong encryption key",
        "wrong master key",
        "encryption error",
        "undecryptable",
    ]
    .iter()
    .any(|n| lower.contains(n));
    let msg = if decrypt_err {
        if raw.contains("cannot decrypt existing store") {
            raw
        } else {
            format!("cannot decrypt existing store — wrong master key or incompatible build; refusing to start fresh: {raw}")
        }
    } else if existing_store {
        format!(
            "cannot open existing store — refusing to start fresh over existing data at {}: {raw}",
            home.display()
        )
    } else {
        format!("core database boot failed: {raw}")
    };
    let path = home.join(LAST_BOOT_ERROR_FILE);
    if let Err(e) = std::fs::write(&path, format!("{}\n", msg.lines().next().unwrap_or(&msg))) {
        tracing::warn!(target: "lastdbd::boot", error = %e, path = %path.display(), "failed to write last_boot_error.txt");
    }
    msg
}

pub(super) fn clear_last_boot_error(home: &Path) {
    let _ = std::fs::remove_file(home.join(LAST_BOOT_ERROR_FILE));
}
