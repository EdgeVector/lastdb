// Proof program for PR 2: contract version compatibility in pick_row()
// Run with: cargo run --example contract_compat_proof

use lastdb_node::app_registry_index::{
    check_manifest_constraint, pick_row, CompatRow, IndexApp, RegistryIndex,
};
use lastdb_node::version::{
    CURRENT_API_VERSION, CURRENT_OUTBOX_FORMAT_VERSION, MIN_SUPPORTED_API_VERSION,
    MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
};
use std::fs;

fn main() {
    println!("=== Contract Version Compatibility Proof ===");
    println!();
    println!(
        "Node: api_version {} (supports {}-{}), outbox_format_version {} (supports {}-{})",
        CURRENT_API_VERSION,
        MIN_SUPPORTED_API_VERSION,
        CURRENT_API_VERSION,
        CURRENT_OUTBOX_FORMAT_VERSION,
        MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
        CURRENT_OUTBOX_FORMAT_VERSION
    );
    println!();

    test_compatible_api_version();
    test_incompatible_api_version();
    test_old_index_exact_match();
    test_prefer_exact_version();
    test_manifest_multi_file_check();
}

fn get_running_version() -> &'static str {
    "0.24.0"
}

fn test_compatible_api_version() {
    println!("## Test 1: Compatible api_version (within range)");
    println!();

    let index = RegistryIndex {
        index_version: 1,
        channel: "stable".to_string(),
        generated_at: "2026-10-24T00:00:00Z".to_string(),
        apps: vec![IndexApp {
            app_id: "brain".to_string(),
            source: "https://github.com/EdgeVector/last-stack".to_string(),
            description: Some("Brain CLI".to_string()),
            compat: vec![CompatRow {
                app_version: "1.0.0".to_string(),
                sha: "abc123".to_string(),
                artifact: None,
                lastdb_version: "0.23.5".to_string(),
                lastdb_api_version: Some(1),
                outbox_format_version: None,
                proved_at: "2026-10-20T00:00:00Z".to_string(),
                proof_run: "ci-run-123".to_string(),
            }],
        }],
    };

    println!(
        "Index has brain with api_version={}, proved on lastdb 0.23.5",
        index.apps[0].compat[0].lastdb_api_version.unwrap()
    );
    println!("Attempting to resolve with lastdb_version=0.24.0 (different from proved)");
    println!();

    match pick_row(
        &index,
        "brain",
        "0.24.0",
        CURRENT_API_VERSION,
        MIN_SUPPORTED_API_VERSION,
        CURRENT_OUTBOX_FORMAT_VERSION,
        MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
    ) {
        Ok((app, row)) => {
            println!("✓ Compatible! Resolved to:");
            println!("  app_id: {}", app.app_id);
            println!("  app_version: {}", row.app_version);
            println!("  sha: {}", row.sha);
            println!("  proved with lastdb: {}", row.lastdb_version);
            println!("  api_version: {}", row.lastdb_api_version.unwrap_or(0));
        }
        Err(e) => {
            println!("✗ UNEXPECTED ERROR: {}", e);
        }
    }
    println!();
    println!("---");
    println!();
}

fn test_incompatible_api_version() {
    println!("## Test 2: Incompatible api_version (too old)");
    println!();

    let index = RegistryIndex {
        index_version: 1,
        channel: "stable".to_string(),
        generated_at: "2026-10-24T00:00:00Z".to_string(),
        apps: vec![IndexApp {
            app_id: "oldapp".to_string(),
            source: "https://github.com/EdgeVector/oldapp".to_string(),
            description: Some("Old app".to_string()),
            compat: vec![CompatRow {
                app_version: "1.0.0".to_string(),
                sha: "old123".to_string(),
                artifact: None,
                lastdb_version: "0.20.0".to_string(),
                lastdb_api_version: Some(0),
                outbox_format_version: None,
                proved_at: "2026-08-01T00:00:00Z".to_string(),
                proof_run: "ci-run-old".to_string(),
            }],
        }],
    };

    println!(
        "Index has oldapp with api_version={} (< MIN_SUPPORTED_API_VERSION={})",
        index.apps[0].compat[0].lastdb_api_version.unwrap(),
        MIN_SUPPORTED_API_VERSION
    );
    println!();

    match pick_row(
        &index,
        "oldapp",
        "0.24.0",
        CURRENT_API_VERSION,
        MIN_SUPPORTED_API_VERSION,
        CURRENT_OUTBOX_FORMAT_VERSION,
        MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
    ) {
        Ok((_, row)) => {
            println!(
                "✗ UNEXPECTED SUCCESS: resolved to version {}",
                row.app_version
            );
        }
        Err(e) => {
            println!("✓ Rejected as expected:");
            println!("  Error: {}", e);
        }
    }
    println!();
    println!("---");
    println!();
}

fn test_old_index_exact_match() {
    println!("## Test 3: Old index (no contract info) requires exact version match");
    println!();

    let index = RegistryIndex {
        index_version: 1,
        channel: "stable".to_string(),
        generated_at: "2026-09-01T00:00:00Z".to_string(),
        apps: vec![IndexApp {
            app_id: "legacy".to_string(),
            source: "https://github.com/EdgeVector/legacy".to_string(),
            description: Some("Legacy app".to_string()),
            compat: vec![CompatRow {
                app_version: "1.0.0".to_string(),
                sha: "legacy123".to_string(),
                artifact: None,
                lastdb_version: "0.23.0".to_string(),
                lastdb_api_version: None,
                outbox_format_version: None,
                proved_at: "2026-09-01T00:00:00Z".to_string(),
                proof_run: "ci-run-legacy".to_string(),
            }],
        }],
    };

    println!("Index has legacy app with NO contract version fields");
    println!(
        "Proved on lastdb: {}",
        index.apps[0].compat[0].lastdb_version
    );
    println!();

    println!("Attempting to resolve with exact version (0.23.0):");
    match pick_row(
        &index,
        "legacy",
        "0.23.0",
        CURRENT_API_VERSION,
        MIN_SUPPORTED_API_VERSION,
        CURRENT_OUTBOX_FORMAT_VERSION,
        MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
    ) {
        Ok((_, row)) => {
            println!("✓ Resolved (exact match works):");
            println!("  app_version: {}", row.app_version);
        }
        Err(e) => {
            println!("✗ UNEXPECTED ERROR: {}", e);
        }
    }
    println!();

    println!("Attempting to resolve with different version (0.24.0):");
    match pick_row(
        &index,
        "legacy",
        "0.24.0",
        CURRENT_API_VERSION,
        MIN_SUPPORTED_API_VERSION,
        CURRENT_OUTBOX_FORMAT_VERSION,
        MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
    ) {
        Ok((_, row)) => {
            println!(
                "✗ UNEXPECTED SUCCESS: resolved to version {}",
                row.app_version
            );
        }
        Err(e) => {
            println!("✓ Rejected as expected (no contract info, no exact match):");
            println!("  Error: {}", e);
        }
    }
    println!();
    println!("---");
    println!();
}

fn test_prefer_exact_version() {
    println!("## Test 4: Prefer exact lastdb_version among compatible rows");
    println!();

    let index = RegistryIndex {
        index_version: 1,
        channel: "stable".to_string(),
        generated_at: "2026-10-24T00:00:00Z".to_string(),
        apps: vec![IndexApp {
            app_id: "brain".to_string(),
            source: "https://github.com/EdgeVector/last-stack".to_string(),
            description: Some("Brain CLI".to_string()),
            compat: vec![
                CompatRow {
                    app_version: "1.0.0".to_string(),
                    sha: "older123".to_string(),
                    artifact: None,
                    lastdb_version: "0.23.0".to_string(),
                    lastdb_api_version: Some(1),
                    outbox_format_version: None,
                    proved_at: "2026-10-20T00:00:00Z".to_string(),
                    proof_run: "ci-run-old".to_string(),
                },
                CompatRow {
                    app_version: "1.1.0".to_string(),
                    sha: "exact123".to_string(),
                    artifact: None,
                    lastdb_version: "0.24.0".to_string(),
                    lastdb_api_version: Some(1),
                    outbox_format_version: None,
                    proved_at: "2026-10-24T00:00:00Z".to_string(),
                    proof_run: "ci-run-exact".to_string(),
                },
            ],
        }],
    };

    println!("Index has brain with two compatible rows:");
    println!("  1. app_version=1.0.0, lastdb_version=0.23.0, api_version=1");
    println!("  2. app_version=1.1.0, lastdb_version=0.24.0, api_version=1");
    println!();
    println!("Resolving with lastdb_version=0.24.0 (matches row 2 exactly):");
    println!();

    match pick_row(
        &index,
        "brain",
        "0.24.0",
        CURRENT_API_VERSION,
        MIN_SUPPORTED_API_VERSION,
        CURRENT_OUTBOX_FORMAT_VERSION,
        MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
    ) {
        Ok((_, row)) => {
            println!("✓ Resolved to:");
            println!("  app_version: {}", row.app_version);
            println!("  sha: {}", row.sha);
            println!("  lastdb_version: {}", row.lastdb_version);
            if row.lastdb_version == "0.24.0" {
                println!("  ✓ Correctly preferred the exact version match!");
            } else {
                println!(
                    "  ✗ WRONG: expected exact match (0.24.0), got {}",
                    row.lastdb_version
                );
            }
        }
        Err(e) => {
            println!("✗ UNEXPECTED ERROR: {}", e);
        }
    }
    println!();
    println!("---");
    println!();
}

fn test_manifest_multi_file_check() {
    println!("## Test 5: Multi-file manifest check (lastdb-app.json + folddb.toml)");
    println!();

    let temp_dir =
        std::env::temp_dir().join(format!("lastdb-manifest-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("create temp dir");

    println!("Testing incompatible constraint in folddb.toml (plain lastdb-app.json doesn't stop the check):");
    println!();

    // Create a plain lastdb-app.json with no version constraint
    let plain_manifest = r#"{
  "app_id": "testapp",
  "metadata": {
    "display_name": "Test App",
    "description": "Test",
    "homepage_url": "https://example.com"
  },
  "version": "1.0.0",
  "schemas": [{"name": "test/Record", "schema_type": "Hash", "fields": ["id"]}]
}"#;
    fs::write(temp_dir.join("lastdb-app.json"), plain_manifest).expect("write manifest");

    // Create folddb.toml with an incompatible constraint
    let incompatible_toml = r#"
[app]
name = "testapp"

lastdb = ">=0.30.0"
"#;
    fs::write(temp_dir.join("folddb.toml"), incompatible_toml).expect("write toml");

    match check_manifest_constraint(&temp_dir, "testapp", get_running_version()) {
        Ok(()) => {
            println!(
                "✗ UNEXPECTED SUCCESS: should have rejected incompatible folddb.toml constraint"
            );
        }
        Err(e) => {
            println!("✓ Rejected as expected:");
            println!("  Error: {}", e);
        }
    }
    println!();

    println!("---");
    println!();

    println!("Testing compatible constraint in folddb.toml:");
    println!();

    // Update folddb.toml with a compatible constraint
    let compatible_toml = r#"
[app]
name = "testapp"

lastdb = ">=0.23.0, <0.30.0"
"#;
    fs::write(temp_dir.join("folddb.toml"), compatible_toml).expect("write toml");

    match check_manifest_constraint(&temp_dir, "testapp", get_running_version()) {
        Ok(()) => {
            println!("✓ Accepted compatible constraint");
        }
        Err(e) => {
            println!("✗ UNEXPECTED ERROR: {}", e);
        }
    }

    fs::remove_dir_all(&temp_dir).expect("cleanup temp dir");
    println!();
    println!("---");
    println!();
}
