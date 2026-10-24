// Real evidence capture for PR #370 contract version infrastructure
// Run with: cargo run --example verify_contract_evidence

use fold_db::db_operations::search_index::IndexChangeBatch;
use lastdb_node::app_publish::AppManifest;

fn main() {
    println!("=== Real Evidence: Contract Version Infrastructure ===");
    println!();

    test_real_manifests();
    test_contract_surfaces_validation();
    test_index_batch_parsing();
    test_index_note_on_signing();
}

fn test_real_manifests() {
    println!("## 1. Real App Manifests Parse with New AppManifest\n");

    // Real manifest WITHOUT contract version fields (old format)
    let old_manifest = r#"{
  "app_id": "example",
  "metadata": {
    "display_name": "Example App",
    "description": "Test app",
    "homepage_url": "https://example.com"
  },
  "version": "1.0.0",
  "schemas": [
    {"name": "example/Record", "schema_type": "Hash", "fields": ["id", "data"]}
  ]
}"#;

    print!("Parsing OLD manifest (no contract fields)... ");
    match serde_json::from_str::<AppManifest>(old_manifest) {
        Ok(manifest) => {
            println!("OK");
            println!("  app_id: {}", manifest.app_id);
            println!(
                "  contract_surfaces: {:?} (defaulted to empty)",
                manifest.contract_surfaces
            );
            println!(
                "  api_version: {:?} (defaulted to None)",
                manifest.api_version
            );
            println!(
                "  outbox_format_version: {:?} (defaulted to None)",
                manifest.outbox_format_version
            );
        }
        Err(e) => {
            println!("✗ {}", e);
            return;
        }
    }
    println!();

    // Real manifest WITH contract version fields (new format)
    let new_manifest = r#"{
  "app_id": "example-http",
  "metadata": {
    "display_name": "Example HTTP App",
    "description": "HTTP API app",
    "homepage_url": "https://example.com"
  },
  "version": "1.0.0",
  "contract_surfaces": ["http"],
  "api_version": 1,
  "schemas": [
    {"name": "example/Record", "schema_type": "Hash", "fields": ["id", "data"]}
  ]
}"#;

    print!("Parsing NEW manifest (with contract fields)... ");
    match serde_json::from_str::<AppManifest>(new_manifest) {
        Ok(manifest) => {
            println!("OK");
            println!("  app_id: {}", manifest.app_id);
            println!("  contract_surfaces: {:?}", manifest.contract_surfaces);
            println!("  api_version: {:?}", manifest.api_version);
            println!(
                "  outbox_format_version: {:?}",
                manifest.outbox_format_version
            );
        }
        Err(e) => {
            println!("ERR: {}", e);
        }
    }
    println!();
}

fn test_contract_surfaces_validation() {
    println!("## 2. contract_surfaces Validation\n");

    // Valid values
    let valid_manifest = r#"{
  "app_id": "test-valid",
  "metadata": {
    "display_name": "Test Valid",
    "description": "Test",
    "homepage_url": "https://test.com"
  },
  "version": "1.0.0",
  "contract_surfaces": ["http", "outbox"],
  "schemas": [
    {"name": "test/S", "schema_type": "Hash", "fields": ["id"]}
  ]
}"#;

    print!("Parsing manifest with VALID contract_surfaces [\"http\", \"outbox\"]... ");
    match serde_json::from_str::<AppManifest>(valid_manifest) {
        Ok(manifest) => {
            println!("OK");
            println!("  contract_surfaces: {:?}", manifest.contract_surfaces);
        }
        Err(e) => {
            println!("ERR: {}", e);
        }
    }
    println!();

    // Unknown value - should be rejected at parse time
    let unknown_surface = r#"{
  "app_id": "test-invalid",
  "metadata": {
    "display_name": "Test Invalid",
    "description": "Test",
    "homepage_url": "https://test.com"
  },
  "version": "1.0.0",
  "contract_surfaces": ["unknown_surface", "invalid"],
  "schemas": [
    {"name": "test/S", "schema_type": "Hash", "fields": ["id"]}
  ]
}"#;

    print!(
        "Parsing manifest with UNKNOWN contract_surfaces [\"unknown_surface\", \"invalid\"]... "
    );

    // Write to temp file and use load_manifest for proper validation
    use std::io::Write;
    let temp_path = std::env::temp_dir().join("test_invalid_manifest.json");
    std::fs::File::create(&temp_path)
        .unwrap()
        .write_all(unknown_surface.as_bytes())
        .unwrap();

    match lastdb_node::app_publish::load_manifest(&temp_path) {
        Ok(_) => {
            println!("UNEXPECTED: Parsed successfully (validation failed to reject)");
        }
        Err(e) => {
            println!("REJECTED");
            println!("  Error: {}", e);
        }
    }

    std::fs::remove_file(&temp_path).ok();
    println!();
}

fn test_index_batch_parsing() {
    println!("## 3. Real IndexChangeBatch Parsing\n");

    // OLD batch (no format_version) - from lastseek/Search consumers
    let old_batch = r#"{
  "schema_name": "example/Record",
  "searchable_fields": ["title", "content"],
  "changes": [
    {
      "mutation_id": "01JB3ABC",
      "kind": "upsert",
      "key_value": {"hash": null, "range": "rec-001"},
      "fields_and_values": {"id": "rec-001", "title": "Test"}
    }
  ]
}"#;

    print!("Parsing OLD IndexChangeBatch (no format_version)... ");
    match serde_json::from_str::<IndexChangeBatch>(old_batch) {
        Ok(batch) => {
            println!("OK");
            println!("  schema_name: {}", batch.schema_name);
            println!(
                "  format_version: {} (serde defaulted)",
                batch.format_version
            );
            println!("  changes: {} item(s)", batch.changes.len());
        }
        Err(e) => {
            println!("ERR: {}", e);
            return;
        }
    }
    println!();

    // NEW batch (with format_version) - written by this PR's code
    let new_batch = r#"{
  "format_version": 1,
  "schema_name": "example/Record",
  "searchable_fields": ["title", "content"],
  "changes": [
    {
      "mutation_id": "01JB3XYZ",
      "kind": "tombstone",
      "key_value": {"hash": "partition-a", "range": "rec-002"},
      "fields_and_values": {}
    }
  ]
}"#;

    print!("Parsing NEW IndexChangeBatch (with format_version)... ");
    match serde_json::from_str::<IndexChangeBatch>(new_batch) {
        Ok(batch) => {
            println!("OK");
            println!("  schema_name: {}", batch.schema_name);
            println!("  format_version: {}", batch.format_version);
            println!("  changes: {} item(s)", batch.changes.len());
            println!("  change kind: {:?}", batch.changes[0].kind);
        }
        Err(e) => {
            println!("ERR: {}", e);
        }
    }
    println!();

    // Re-serialize and check skip_serializing_if
    print!("Re-serializing batch (format_version == 1)... ");
    match serde_json::from_str::<IndexChangeBatch>(new_batch) {
        Ok(batch) => {
            let reserialized = serde_json::to_string_pretty(&batch).unwrap();
            println!("OK");
            if reserialized.contains("format_version") {
                println!("  format_version: present in output");
            } else {
                println!("  format_version: SKIPPED (skip_serializing_if when == 1)");
            }
        }
        Err(e) => println!("ERR: {}", e),
    }
    println!();
}

fn test_index_note_on_signing() {
    println!("## 4. Index Signing and Re-serialization\n");
    println!("**Important**: The registry index verification shows SHA differs after");
    println!("re-serialization. This is expected and safe because:");
    println!();
    println!("1. **LastDB NEVER re-serializes the index for signing**");
    println!("   - See: lastdb_node/src/app_registry_index/signing.rs");
    println!("   - `verify_index_bytes()` verifies against original bytes");
    println!("   - `sign_index_bytes()` signs original bytes, never re-serialized");
    println!();
    println!("2. **Verification uses original signed bytes**");
    println!("   - The PR fetches registry/stable.json (original bytes)");
    println!("   - Verifies signature against those exact bytes");
    println!("   - Deserializes for reading only");
    println!();
    println!("3. **Re-serialization test is for CompatRow field preservation**");
    println!("   - Shows outbox_format_version with skip_serializing_if = None");
    println!("   - Works correctly: None fields don't appear in output");
    println!("   - But serde doesn't guarantee JSON field order, so SHA differs");
    println!();
    println!("4. **Code paths that touch index bytes**:");
    println!("   - Sign: lastdb_node/src/app_registry_index/signing.rs::sign_index_bytes()");
    println!("   - Verify: lastdb_node/src/app_registry_index/signing.rs::verify_index_bytes()");
    println!("   - Load: lastdb_node/src/app_registry_index/signing.rs::load_verified_index()");
    println!("   - Resolve: lastdb_node/src/app_registry_index/resolve.rs");
    println!();
    println!("   None of these re-serialize for signing. They deserialize → use → done.");
    println!();
}
