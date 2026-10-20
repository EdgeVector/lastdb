//! Human output of `lastdb liveness explain molecule`.

/// One block for each molecule: the three counters and the state. A molecule
/// may go only when all three are zero and none is suspect.
pub(super) fn print_molecule_liveness(molecules: &[serde_json::Value]) {
    for molecule in molecules {
        let text = |name: &str| {
            molecule
                .get(name)
                .and_then(serde_json::Value::as_str)
                .unwrap_or("?")
                .to_string()
        };
        let number = |name: &str| {
            molecule
                .get(name)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        let flag = |name: &str| {
            molecule
                .get(name)
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        };
        println!("Liveness: molecule {}", text("molecule"));
        println!("  State: {}", text("state"));
        println!(
            "  Droppable (all three counters are zero): {}",
            flag("droppable")
        );
        println!("  Slots (tip records): {}", number("slots"));
        println!("  Records (conflict records): {}", number("records"));
        println!(
            "  Bindings (schema field, protein member, database catalog): {}",
            number("bindings")
        );
        println!("  Suspect: {}", flag("suspect"));
        println!("  Header present: {}", flag("header_present"));
        if let Some(records) = molecule
            .get("binding_records")
            .and_then(serde_json::Value::as_array)
        {
            println!(
                "  Binding records: {} (counter agrees: {})",
                records.len(),
                flag("binding_records_agree")
            );
        }
    }
}
