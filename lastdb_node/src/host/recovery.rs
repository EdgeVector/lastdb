//! Boot recovery-phrase emission, Spotlight opt-out, and `last_boot_error.txt`.

use std::fmt;
use std::io::{self, IsTerminal};
use std::path::Path;

pub const LAST_BOOT_ERROR_FILE: &str = "last_boot_error.txt";

pub(super) fn emit_fresh_recovery_phrase(mnemonic: &str) {
    let suppress = std::env::var_os("CI").is_some()
        || std::env::var_os("LASTDB_SUPPRESS_RECOVERY_PHRASE").is_some();
    if io::stderr().is_terminal() && !suppress {
        eprint!(
            "\n=== LastDB recovery phrase (save this; only shown once) ===\n{mnemonic}\n===========================================================\nThis phrase is your account root: identity, content encryption, and HashKey blinding. Enabling cloud later reuses it — no re-encryption of field keys.\n"
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
        let m = home.join(".metadata_never_index");
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&m)
        {
            Ok(_) => tracing::info!(path = %m.display(), "excluded node home from Spotlight"),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                tracing::warn!(target: "lastdb_node::host", path = %m.display(), error = %e, "could not exclude node home from Spotlight");
            }
        }
    }
    Ok(())
}

pub(super) fn map_and_record_boot_error(
    home: &Path,
    existing: bool,
    err: impl fmt::Display,
) -> String {
    let raw = err.to_string();
    let lower = raw.to_ascii_lowercase();
    let keys =
        "aead|aes-gcm|decrypt|wrong encryption key|wrong master key|encryption error|undecryptable";
    let decrypt_err = keys.split('|').any(|n| lower.contains(n));
    let msg = if decrypt_err && raw.contains("cannot decrypt existing store") {
        raw
    } else if decrypt_err {
        format!("cannot decrypt existing store — wrong master key or incompatible build; refusing to start fresh: {raw}")
    } else if existing {
        format!(
            "cannot open existing store — refusing to start fresh over existing data at {}: {raw}",
            home.display()
        )
    } else {
        format!("core database boot failed: {raw}")
    };
    let line = format!("{}\n", msg.lines().next().unwrap_or(&msg));
    if let Err(e) = std::fs::write(home.join(LAST_BOOT_ERROR_FILE), line) {
        tracing::warn!(target: "lastdbd::boot", error = %e, "failed to write last_boot_error.txt");
    }
    msg
}
