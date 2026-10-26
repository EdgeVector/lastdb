//! Tests for legacy apps.json handling and migration.

use crate::helpers::make_app;
use schema_service_core::external_persistence::ExternalSchemaPersistence;
use schema_service_s3::S3BlobPersistence;

pub async fn test_legacy_app_takeover(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 5: Legacy apps.json takeover prevention ---");

    let app_id = format!("legacy-app-{}", chrono::Utc::now().timestamp());
    let owner_a = "legacy_owner_a_pubkey";
    let owner_b = "legacy_owner_b_pubkey";

    let legacy_app = make_app(&app_id, owner_a, "Legacy App A");
    let mut legacy_blob = std::collections::HashMap::new();
    legacy_blob.insert(app_id.clone(), legacy_app.clone());

    let legacy_json = serde_json::to_vec(&serde_json::json!({
        "items": legacy_blob,
        "version": 1
    }))?;

    println!(
        "Seeding legacy apps.json with app '{}' owned by A...",
        app_id
    );
    backend
        .client()
        .put_object()
        .bucket(backend.bucket())
        .key("apps.json")
        .body(aws_sdk_s3::primitives::ByteStream::from(legacy_json))
        .send()
        .await?;
    println!("✓ Legacy blob seeded (no apps/{}.json exists yet)", app_id);

    let app_b = make_app(&app_id, owner_b, "App B");
    println!("\nDeveloper B attempts to create app '{}'...", app_id);
    match backend.save_app(&app_b).await {
        Err(e) if e.to_string().contains("different owner") => {
            println!("✓ Developer B rejected: {}", e);
        }
        Err(e) => {
            println!("✗ FAIL: Wrong error: {}", e);
            return Err(e.into());
        }
        Ok(_) => {
            println!("✗ FAIL: Developer B should have been rejected (legacy takeover)");
            return Err("Legacy app takeover allowed".into());
        }
    }

    let app_a = make_app(&app_id, owner_a, "App A");
    println!(
        "\nDeveloper A creates app '{}' (idempotent with legacy)...",
        app_id
    );
    backend.save_app(&app_a).await?;
    println!("✓ Developer A succeeded (idempotent with legacy owner)");

    println!("\nDeveloper A updates app metadata (triggers migration)...");
    let mut app_a_updated = app_a.clone();
    app_a_updated.metadata.description = "Updated description".to_string();
    backend.update_app(&app_a_updated).await?;
    println!("✓ Update succeeded, app migrated to apps/{}.json", app_id);

    let migrated = backend
        .load_app(&app_id)
        .await?
        .ok_or("App not found after migration")?;
    if migrated.owner_dev_pubkey != owner_a {
        println!("✗ FAIL: Migrated app has wrong owner");
        return Err("Migration owner mismatch".into());
    }
    println!("✓ Verified: apps/{}.json exists with owner A", app_id);

    println!(
        "\nDeveloper B attempts to create app '{}' after migration...",
        app_id
    );
    match backend.save_app(&app_b).await {
        Err(e) if e.to_string().contains("different owner") => {
            println!("✓ Developer B still rejected: {}\n", e);
            Ok(())
        }
        Err(e) => {
            println!("✗ FAIL: Wrong error: {}", e);
            Err(e.into())
        }
        Ok(_) => {
            println!("✗ FAIL: Developer B should have been rejected after migration");
            Err("Post-migration takeover allowed".into())
        }
    }
}
