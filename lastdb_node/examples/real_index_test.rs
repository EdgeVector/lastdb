// Test with the real published stable.json from homebrew-lastdb
// Run with: cargo run --example real_index_test

use lastdb_node::app_registry_index::{parse_index, pick_row};
use lastdb_node::version::{
    CURRENT_API_VERSION, CURRENT_OUTBOX_FORMAT_VERSION, MIN_SUPPORTED_API_VERSION,
    MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
};

fn main() {
    println!("=== Real Published Index Test ===");
    println!();

    let index_bytes = include_bytes!("../../test-data/stable.json");
    
    match parse_index(index_bytes) {
        Ok(index) => {
            println!("✓ Real index parsed successfully");
            println!("  channel: {}", index.channel);
            println!("  generated_at: {}", index.generated_at);
            println!("  apps: {}", index.apps.len());
            println!();

            test_brain_resolve(&index);
            test_lastsecrets_resolve(&index);
        }
        Err(e) => {
            println!("✗ Failed to parse real index: {}", e);
            std::process::exit(1);
        }
    }
}

fn test_brain_resolve(index: &lastdb_node::app_registry_index::RegistryIndex) {
    println!("## Test: Resolve brain from real index");
    println!();

    let brain_app = index.apps.iter().find(|a| a.app_id == "brain");
    match brain_app {
        Some(app) => {
            println!("Found brain app with {} compat rows", app.compat.len());
            
            // Show first row details
            if let Some(row) = app.compat.first() {
                println!("  First row:");
                println!("    app_version: {}", row.app_version);
                println!("    lastdb_version: {}", row.lastdb_version);
                println!("    api_version: {:?}", row.lastdb_api_version);
                println!("    outbox_format_version: {:?}", row.outbox_format_version);
            }
            println!();
        }
        None => {
            println!("✗ brain app not found in index");
            return;
        }
    }

    // Try to resolve brain with a version from the index
    let test_version = "0.23.3-2127-gaf73aa569";
    println!("Attempting to resolve brain with lastdb_version={}", test_version);
    
    match pick_row(
        index,
        "brain",
        test_version,
        CURRENT_API_VERSION,
        MIN_SUPPORTED_API_VERSION,
        CURRENT_OUTBOX_FORMAT_VERSION,
        MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
    ) {
        Ok((app, row)) => {
            println!("✓ Successfully resolved:");
            println!("  app_id: {}", app.app_id);
            println!("  app_version: {}", row.app_version);
            println!("  sha: {}", row.sha);
            println!("  proved with: {}", row.lastdb_version);
        }
        Err(e) => {
            println!("✗ Resolution failed: {}", e);
        }
    }
    println!();
    println!("---");
    println!();
}

fn test_lastsecrets_resolve(index: &lastdb_node::app_registry_index::RegistryIndex) {
    println!("## Test: Resolve lastsecrets from real index");
    println!();

    let lastsecrets_app = index.apps.iter().find(|a| a.app_id == "lastsecrets");
    match lastsecrets_app {
        Some(app) => {
            println!("Found lastsecrets app with {} compat rows", app.compat.len());
            
            // Show first row details
            if let Some(row) = app.compat.first() {
                println!("  First row:");
                println!("    app_version: {}", row.app_version);
                println!("    lastdb_version: {}", row.lastdb_version);
                println!("    api_version: {:?}", row.lastdb_api_version);
                println!("    outbox_format_version: {:?}", row.outbox_format_version);
            }
            println!();
        }
        None => {
            println!("✗ lastsecrets app not found in index");
            return;
        }
    }

    // Try to resolve lastsecrets with a version from the index
    let test_version = "0.23.3-2127-gaf73aa569";
    println!("Attempting to resolve lastsecrets with lastdb_version={}", test_version);
    
    match pick_row(
        index,
        "lastsecrets",
        test_version,
        CURRENT_API_VERSION,
        MIN_SUPPORTED_API_VERSION,
        CURRENT_OUTBOX_FORMAT_VERSION,
        MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
    ) {
        Ok((app, row)) => {
            println!("✓ Successfully resolved:");
            println!("  app_id: {}", app.app_id);
            println!("  app_version: {}", row.app_version);
            println!("  sha: {}", row.sha);
            println!("  proved with: {}", row.lastdb_version);
        }
        Err(e) => {
            println!("✗ Resolution failed: {}", e);
        }
    }
    println!();
    println!("---");
    println!();
}
