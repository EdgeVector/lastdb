//! Human output of `lastdb liveness explain blob`.

/// One block for each blob: the counter, the state and the dwell. A blob has no
/// list of referrers; the counter holds the number of atom counters that list
/// the blob plus the live tip records that name it.
pub(super) fn print_blob_liveness(blobs: &[serde_json::Value], dwell_secs: u64) {
    for blob in blobs {
        let id = blob
            .get("blob")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?");
        let state = blob
            .get("state")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let refs = blob
            .get("refs")
            .and_then(serde_json::Value::as_u64)
            .map_or_else(|| "none".to_string(), |refs| refs.to_string());
        let flag = |name: &str| {
            blob.get(name)
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        };
        println!("Liveness: blob {id}");
        println!("  State: {state}");
        println!("  Counter (atom counters and tip records that name the blob): {refs}");
        match blob
            .get("zero_since_unix_ms")
            .and_then(serde_json::Value::as_u64)
        {
            Some(at) => println!("  Zero since (unix ms): {at} (dwell {dwell_secs} s)"),
            None => println!("  Zero since: none"),
        }
        println!("  Queued for the dwell pass: {}", flag("queued"));
        println!("  Suspect: {}", flag("suspect"));
        println!("  Blob row present: {}", flag("row_present"));
        if state == "referenced_row_missing" {
            println!(
                "  The atoms name a blob that is not in the store (a write named it after its dwell). Upload the file again and write the atom again."
            );
        }
        if flag("unreadable") {
            println!("  The counter record does not decode. Run `lastdb db recount`.");
        }
    }
}
