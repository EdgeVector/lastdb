//! `lastdb db molecule-drain`: name a molecule to the drain, and show its report.

use super::*;
use serde_json::{json, Value};

fn u64_of(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

/// Post the request and print the verdict and the report. A molecule that the
/// drain refuses makes the command exit non-zero, so a script sees the fault.
pub(crate) fn db_molecule_drain(
    socket: &Path,
    molecule: Option<&str>,
    scope: Option<&str>,
    json_out: bool,
) -> Result<(), String> {
    let mut body = json!({});
    if let Some(molecule) = molecule {
        body["molecule"] = Value::from(molecule);
    }
    if let Some(scope) = scope {
        body["scope"] = Value::from(scope);
    }
    let value = db_post_json(socket, "/api/db/molecule-drain", &body)?;
    let drain = value
        .get("molecule_drain")
        .ok_or_else(|| "response missing molecule_drain".to_string())?;
    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(drain).map_err(|e| e.to_string())?
        );
    } else {
        print_drain(drain);
    }
    let refused = drain
        .get("verdict")
        .and_then(|verdict| verdict.get("state"))
        .and_then(Value::as_str)
        == Some("refused");
    if refused {
        return Err("the drain refuses this molecule: a counter or a record keeps it".into());
    }
    Ok(())
}

fn print_drain(drain: &Value) {
    if let Some(verdict) = drain.get("verdict").filter(|verdict| !verdict.is_null()) {
        let text = |key: &str| verdict.get(key).and_then(Value::as_str).unwrap_or("");
        println!("Molecule: {}", text("molecule"));
        let scope = text("scope");
        println!(
            "Scope: {}",
            if scope.is_empty() { "personal" } else { scope }
        );
        println!("State: {}", text("state"));
        println!(
            "Slots (tip records): {}  Records (kept conflict records): {}",
            u64_of(verdict, "slots"),
            u64_of(verdict, "records")
        );
        if let Some(why) = verdict.get("refusal").and_then(Value::as_str) {
            println!("Refused: {why}");
        }
        println!(
            "Queued for the drain: {}",
            verdict
                .get("queued")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        );
    }
    let Some(report) = drain.get("report") else {
        return;
    };
    println!(
        "Drain: pending {} parked {} (waiting for kept records {}, refused {}); drained {} molecules; tips erased {}; atom bodies deleted {}; other rows {}; batches {}; rebound {}",
        u64_of(report, "pending"),
        u64_of(report, "parked"),
        u64_of(report, "waiting_records"),
        u64_of(report, "refused"),
        u64_of(report, "drained"),
        u64_of(report, "tips_erased"),
        u64_of(report, "atoms_deleted"),
        u64_of(report, "rows_removed"),
        u64_of(report, "batches"),
        u64_of(report, "rebound"),
    );
    if let Some(error) = report.get("last_error").and_then(Value::as_str) {
        println!("Last error: {error}");
    }
}
