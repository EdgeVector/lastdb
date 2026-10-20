//! Human output of `lastdb liveness explain atom`.

/// One block for each atom: the counter, the state and the body. An atom has no
/// list of referrers; the counter holds the number of tip and conflict records.
pub(super) fn print_atom_liveness(atoms: &[serde_json::Value]) {
    for atom in atoms {
        let id = atom
            .get("atom")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?");
        let state = atom
            .get("state")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let refs = atom
            .get("refs")
            .and_then(serde_json::Value::as_u64)
            .map_or_else(|| "none".to_string(), |refs| refs.to_string());
        let flag = |name: &str| {
            atom.get(name)
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        };
        let blobs = atom
            .get("blobs")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        println!("Liveness: atom {id}");
        println!("  State: {state}");
        println!("  Counter (tip and conflict records that name the atom): {refs}");
        println!("  Suspect: {}", flag("suspect"));
        println!("  Body present: {}", flag("body_present"));
        println!("  Blob references: {blobs}");
    }
}
