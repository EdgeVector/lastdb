//! Byte formatting, response parsing and hash-group migration.

use super::*;

pub(crate) fn format_bytes(n: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let f = n as f64;
    if f >= GIB {
        format!("{:.2} GiB", f / GIB)
    } else if f >= MIB {
        format!("{:.1} MiB", f / MIB)
    } else if f >= KIB {
        format!("{:.1} KiB", f / KIB)
    } else {
        format!("{n} B")
    }
}

pub(crate) fn parse_json_response(
    response: &str,
    label: &str,
) -> Result<serde_json::Value, String> {
    let status_line = response.lines().next().unwrap_or("<empty response>");
    if !response.starts_with("HTTP/1.1 200 ") {
        let body = response
            .split_once("\r\n\r\n")
            .map_or("", |(_, b)| b.trim());
        return Err(format!("{label} returned {status_line}: {body}"));
    }
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| format!("{label} response had no body"))?;
    serde_json::from_str(body).map_err(|e| format!("invalid {label} JSON: {e}"))
}

pub(crate) fn parse_binary_response(response: &[u8], label: &str) -> Result<Vec<u8>, String> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| format!("{label} response had no headers"))?;
    let header = std::str::from_utf8(&response[..header_end])
        .map_err(|e| format!("invalid {label} response headers: {e}"))?;
    let status_line = header.lines().next().unwrap_or("<empty response>");
    if !status_line.starts_with("HTTP/1.1 200 ") {
        let body = String::from_utf8_lossy(&response[header_end + 4..]);
        return Err(format!("{label} returned {status_line}: {}", body.trim()));
    }
    Ok(response[header_end + 4..].to_vec())
}

pub(crate) fn migrate_hash_group_command(
    data_dir: Option<PathBuf>,
    from: &Path,
    into: &Path,
    hash_group_key: HashGroupKeyArg,
    partition_fanout: u32,
    json_only: bool,
) -> Result<(), String> {
    // lint:fn-size-ok moved verbatim from the original file; splitting is a separate change
    if !partition_fanout.is_power_of_two() {
        return Err(format!(
            "--partition-fanout must be a power of two, got {partition_fanout}"
        ));
    }
    if hash_group_key == HashGroupKeyArg::FullKey && partition_fanout != 1 {
        return Err(
            "--partition-fanout only applies to --hash-group-key partition-prefix".to_string(),
        );
    }
    let protected_primary = lastdb_node::host::resolve_home(data_dir)?;
    let source_home = expand_home_path(from)?;
    let target_home = expand_home_path(into)?;
    refuse_same_home(&source_home, &target_home)?;
    refuse_same_home(&protected_primary, &target_home)
        .map_err(|_| "refusing to migrate into the configured primary LastDB home".to_string())?;
    refuse_non_fresh_migration_home(&target_home)?;

    let source_socket = lastdb_uds::uds::socket_path(&source_home.join("data"));
    if lastdb_node::health_alert::probe_health(&source_socket).is_ok() {
        return Err(format!(
            "refusing to migrate live source {}; stop it or use an immutable CoW copy",
            source_home.display()
        ));
    }
    let source_data = source_home.join("data");
    if !source_data.is_dir() {
        return Err(format!(
            "source LastStore data directory does not exist: {}",
            source_data.display()
        ));
    }
    let identity_path = source_home.join(lastdb_node::host::IDENTITY_KEY_FILE);
    let identity = std::fs::read(&identity_path)
        .map_err(|e| format!("read {}: {e}", identity_path.display()))?;
    if identity.len() != 32 {
        return Err(format!(
            "{} must be 32 bytes, got {}",
            identity_path.display(),
            identity.len()
        ));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&identity);
    let e2e = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
        .map_err(|e| format!("E2E derive: {e}"))?;
    let data_key = e2e.encryption_key();

    let mut source_options = laststore::LastStoreOptions::default();
    if laststore_home_has_chunk_layout(&source_data) {
        source_options.data_key = Some(data_key);
    }
    let source = laststore::LastStore::open_existing_or_with(&source_data, source_options)
        .map_err(|e| format!("open source LastStore: {e}"))?;
    let source_layout = source.options().layout_mode;
    // Two supported sources. `segment_log` is the original conversion, which
    // must strip the legacy full-value ENC envelope on the way through.
    // `hash_group` is a *relayout*: only physical placement changes, so values
    // are copied byte-for-byte — on a plain hash-group home the value ENC seam
    // lives above LastStore, and re-opening it here would corrupt the copy.
    let relayout = match source_layout {
        laststore::LayoutMode::SegmentLog => false,
        laststore::LayoutMode::HashGroup => true,
    };
    if relayout && source.options().packaging != laststore::PackagingMode::Plain {
        // migrate_to_hash_group_with always writes a plain destination. Silently
        // converting a frame-AEAD home would drop its at-rest frame protection.
        return Err(format!(
            "refusing to relayout a {:?}-packaged hash-group home: the destination \
             would be plain, dropping frame AEAD at-rest protection",
            source.options().packaging
        ));
    }
    if relayout
        && source.options().hash_group_key == laststore::HashGroupKey::from(hash_group_key)
        && source.options().hash_group_partition_fanout == partition_fanout
    {
        return Err(format!(
            "source already uses hash_group_key={} partition_fanout={}; nothing to relayout",
            hash_group_key.as_str(),
            partition_fanout
        ));
    }

    std::fs::create_dir_all(&target_home)
        .map_err(|e| format!("create {}: {e}", target_home.display()))?;
    let target_data = target_home.join("data");
    // Plain packaging: no frame AEAD. Strip legacy full-value ENC envelopes only.
    // Do NOT field-seal atom content here: seal_at_rest uses a random nonce, so
    // migration parity (transform(source) == target.get) would fail. Content is
    // dual-read plain after migrate and sealed on next AtomStore write.
    let mut target_options = laststore::LastStoreOptions::hash_group()
        .with_hash_group_key(hash_group_key.into())
        .with_hash_group_partition_fanout(partition_fanout);
    if relayout {
        // A relayout changes placement only; keep the group count it was sized
        // for, so per-group size stays comparable to the source.
        target_options.hash_group_bits = source.options().hash_group_bits;
    }
    if partition_fanout > 1u32 << target_options.hash_group_bits {
        return Err(format!(
            "--partition-fanout {partition_fanout} exceeds the {} groups implied by \
             hash_group_bits={}",
            1u32 << target_options.hash_group_bits,
            target_options.hash_group_bits
        ));
    }
    let report = source
        .migrate_to_hash_group_with(
            &target_data,
            target_options.clone(),
            |collection, id, body| {
                if relayout {
                    return Ok(body.to_vec());
                }
                fold_db::crypto::open_at_rest(&data_key, body).map_err(|e| {
                    laststore::Error::Config(format!(
                        "open legacy value envelope for {collection}/{id}: {e}"
                    ))
                })
            },
        )
        .map_err(|e| format!("migrate LastStore layout: {e}"))?;
    let migrated = laststore::LastStore::open_existing_or_with(&target_data, target_options)
        .map_err(|e| format!("reopen migrated LastStore: {e}"))?;
    migrated
        .verify_integrity()
        .map_err(|e| format!("verify migrated LastStore: {e}"))?;

    write_owner_only_local(
        &target_home.join(lastdb_node::host::IDENTITY_KEY_FILE),
        &identity,
    )?;

    let payload = serde_json::json!({
        "status": "PASS",
        "source_home": source_home,
        "destination_home": target_home,
        "source_layout": if relayout { "hash_group" } else { "segment_log" },
        "destination_layout": "hash_group",
        "relayout": relayout,
        "hash_group_key": hash_group_key.as_str(),
        "hash_group_partition_fanout": partition_fanout,
        "hash_group_bits": migrated.options().hash_group_bits,
        "layout_epoch": migrated.options().layout_epoch,
        "total_documents": report.total_documents,
        "collections": report.collections,
        "source_unchanged": true,
        "cloud_sync_copied": false,
        "promoted": false,
    });
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .map_err(|e| format!("encode migration report: {e}"))?
        );
    } else {
        if relayout {
            println!("PASS: relaid out hash-group LastStore placement");
        } else {
            println!("PASS: migrated LastStore into hash-group layout");
        }
        println!("  source:      {}", source_home.display());
        println!("  destination: {}", target_home.display());
        println!("  documents:   {}", report.total_documents);
        println!("  collections: {}", report.collections.len());
        println!(
            "  placement:   hash_group_key={} partition_fanout={} groups={}",
            hash_group_key.as_str(),
            partition_fanout,
            1u32 << migrated.options().hash_group_bits
        );
        println!("  layout epoch: {}", migrated.options().layout_epoch);
        println!("  source unchanged; destination not promoted; cloud sync not copied");
    }
    Ok(())
}
