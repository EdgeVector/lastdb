use super::*;

/// Whether stderr may receive a raw recovery phrase.
///
/// CI logs and cargo-test captures are retained. Recovery material must never
/// appear there, even for ephemeral test identities
/// (`papercut-fold-ci-test-recovery-phrase-leaks-to-logs-20260817`).
pub fn should_emit_recovery_phrase() -> bool {
    if cfg!(test) {
        return false;
    }
    if std::env::var_os("CI").is_some() {
        return false;
    }
    if std::env::var_os("LASTDB_SUPPRESS_RECOVERY_PHRASE").is_some() {
        return false;
    }
    io::stderr().is_terminal()
}

/// Lines written when a fresh identity is minted. `emit_secret=false` is the
/// test/CI/non-TTY path and must never contain the mnemonic.
pub fn format_recovery_phrase_notice(mnemonic: &str, emit_secret: bool) -> String {
    if emit_secret {
        format!(
            "\n\
             === LastDB recovery phrase (save this; only shown once) ===\n\
             {mnemonic}\n\
             ===========================================================\n\
             This phrase is your account root: identity, content encryption, \
             and HashKey blinding. Enabling cloud later reuses it — \
             no re-encryption of molecule keys.\n"
        )
    } else {
        "=== LastDB recovery phrase generated (redacted; not printed in test/CI/non-TTY) ===\n"
            .to_string()
    }
}

/// Print a freshly generated recovery phrase, or a redacted stand-in.
pub fn emit_fresh_recovery_phrase(mnemonic: &str) {
    let emit_secret = should_emit_recovery_phrase();
    eprint!("{}", format_recovery_phrase_notice(mnemonic, emit_secret));
    if emit_secret {
        tracing::info!("fresh identity generated; recovery phrase printed on stderr");
    } else {
        tracing::info!("fresh identity generated; recovery phrase redacted (test/CI/non-TTY)");
    }
}

/// File under the node home recording the last boot failure message so
/// `lastdb status` / operators can see why a KeepAlive-restarted service is
/// not serving (without digging through rotated logs).
pub const LAST_BOOT_ERROR_FILE: &str = "last_boot_error.txt";

/// macOS Spotlight opt-out marker. An empty file with this name excludes the
/// directory it sits in — and everything below it — from Spotlight indexing.
pub const SPOTLIGHT_EXCLUSION_FILE: &str = ".metadata_never_index";

/// Create `home` if needed and keep it out of the macOS Spotlight index.
///
/// Every path that can materialize a *live* node home routes through this, so a
/// home is never left indexable just because `cloud connect` created it before
/// the first [`Host::boot`].
pub fn ensure_node_home(home: &Path) -> io::Result<()> {
    std::fs::create_dir_all(home)?;
    mark_never_indexed(home);
    Ok(())
}

/// Write [`SPOTLIGHT_EXCLUSION_FILE`] at the node-home root. Idempotent; never
/// truncates an existing marker, so an operator can put notes in it.
///
/// Two reasons this is a default rather than a tuning knob:
///
/// * **At-rest hygiene.** The run files are encrypted, but some files in the
///   home are plain text, for example the Search inbox batches. Spotlight
///   copies indexed content into a system-wide index that sits *outside* the
///   home and outside any LastDB access control.
/// * **Read-path cost.** Each commit writes a new run file and background
///   merges rewrite them, and every new file is fresh re-index work against the
///   same disk the read path uses.
///
/// Best-effort by construction: an unwritable home is reported by the
/// `create_dir_all` in [`ensure_node_home`], and a missing marker must never
/// fail a boot.
///
/// Deliberately *not* called for `restore --into` destinations: those homes
/// pick the marker up on their first real boot.
#[cfg(target_os = "macos")]
pub fn mark_never_indexed(home: &Path) {
    let marker = home.join(SPOTLIGHT_EXCLUSION_FILE);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
    {
        Ok(_) => tracing::info!(
            path = %marker.display(),
            "excluded node home from Spotlight indexing"
        ),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => tracing::warn!(
            target: "lastdb_node::host",
            path = %marker.display(),
            error = %e,
            "could not exclude node home from Spotlight indexing; \
             plain text files in the home may be copied into the system index"
        ),
    }
}

/// No-op off macOS: Spotlight is the only indexer this marker speaks to.
/// Porting seam for a Windows Search / tracker3 equivalent.
#[cfg(not(target_os = "macos"))]
pub fn mark_never_indexed(_home: &Path) {}

/// The layout descriptor that the deleted LastStore engine wrote into its root
/// on first open. A home from that engine still has it, so the check below
/// still refuses to start fresh over one.
pub(super) const LASTSTORE_LAYOUT_FILE: &str = "laststore-layout-v1";

/// The node marker. Every native node home has it, in disk format v1 and v2.
pub(super) const NODE_MARKER_FILE: &str = "laststore-pack-node-v1";

/// Whether `<home>/data` already holds a store (a layout file, a node marker, or a collection).
/// Used to refuse cloud-bootstrap-as-heal and to keep error messages precise.
///
/// Recognises the layouts the node wrote or writes: the LastStore layout file,
/// the node marker (disk format v1 and v2), or any collection directory under
/// `<data>/data`. The refuse-to-start-fresh contract below rests on this
/// structural answer, not on the storage layer's error text.
pub fn data_dir_has_existing_store(data_path: &Path) -> bool {
    // LastStore: `<data>/laststore-layout-v1` + `<data>/data/<collection>/…`.
    if data_path.join(LASTSTORE_LAYOUT_FILE).is_file() {
        return true;
    }
    // A disk format v2 home has the node marker and no layout file.
    if data_path.join(NODE_MARKER_FILE).is_file() {
        return true;
    }
    let collections = data_path.join("data");
    std::fs::read_dir(&collections)
        .is_ok_and(|entries| entries.filter_map(Result::ok).any(|e| e.path().is_dir()))
}

pub(super) fn is_decrypt_boot_failure(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("cannot decrypt existing store")
        || lower.contains("aead")
        || lower.contains("aes-gcm")
        || lower.contains("decrypt")
        || lower.contains("wrong encryption key")
        || lower.contains("wrong master key")
        || lower.contains("encryption error")
        || lower.contains("undecryptable")
}

/// Map a core boot error into the loud refuse-to-boot message required by
/// incident-lastdbd-0226-wrong-key-fresh-db, and persist it for operators.
pub(super) fn map_and_record_boot_error(
    home: &Path,
    existing_store: bool,
    err: impl std::fmt::Display,
) -> String {
    let raw = err.to_string();
    let msg = if is_decrypt_boot_failure(&raw) {
        if raw.contains("cannot decrypt existing store") {
            raw
        } else {
            format!(
                "cannot decrypt existing store — wrong master key or incompatible build; \
                 refusing to start fresh: {raw}"
            )
        }
    } else if existing_store {
        // Structural, not lexical: `is_decrypt_boot_failure` is a substring
        // heuristic over someone else's error text, so a re-worded storage
        // error would silently demote a wrong-key boot over real data to a
        // generic failure. If the home held data before this process touched
        // it, any boot failure is a refuse-to-start-fresh event regardless of
        // how the underlying layer phrased it.
        format!(
            "cannot open existing store — refusing to start fresh over existing data at {}: {raw}",
            home.display()
        )
    } else {
        format!("core database boot failed: {raw}")
    };
    record_last_boot_error(home, &msg);
    msg
}

pub(super) fn record_last_boot_error(home: &Path, msg: &str) {
    let path = home.join(LAST_BOOT_ERROR_FILE);
    let body = format!("{}\n", msg.lines().next().unwrap_or(msg));
    if let Err(e) = std::fs::write(&path, body) {
        tracing::warn!(
            target: "lastdbd::boot",
            error = %e,
            path = %path.display(),
            "failed to write last_boot_error.txt"
        );
    }
}

pub(super) fn clear_last_boot_error(home: &Path) {
    let path = home.join(LAST_BOOT_ERROR_FILE);
    let _ = std::fs::remove_file(path);
}
