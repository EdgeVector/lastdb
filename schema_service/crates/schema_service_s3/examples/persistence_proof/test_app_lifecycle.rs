//! Tests for app creation and owner isolation.

use crate::helpers::make_app;
use schema_service_core::external_persistence::ExternalSchemaPersistence;
use schema_service_s3::S3BlobPersistence;

pub async fn test_concurrent_create(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 1: Concurrent create (first-write-wins) ---");

    let app_id = format!("app-{}", chrono::Utc::now().timestamp());
    let owner_a = "owner_a_pubkey";
    let owner_b = "owner_b_pubkey";

    let app_a = make_app(&app_id, owner_a, "App A");
    let app_b = make_app(&app_id, owner_b, "App B");

    println!("Creating app with owner A...");
    backend.save_app(&app_a).await?;
    println!("✓ Owner A succeeded");

    println!("Attempting concurrent create with owner B...");
    match backend.save_app(&app_b).await {
        Err(e) if e.to_string().contains("different owner") => {
            println!("✓ Owner B rejected: {e}");
        }
        Err(e) => {
            println!("✗ FAIL: Wrong error: {e}");
            return Err(e.into());
        }
        Ok(_) => {
            println!("✗ FAIL: Owner B should have been rejected");
            return Err("Concurrent create allowed takeover".into());
        }
    }

    let stored = backend.load_app(&app_id).await?.ok_or("App not found")?;
    if stored.owner_dev_pubkey != owner_a {
        println!("✗ FAIL: Stored owner is not A");
        return Err("Wrong owner".into());
    }
    println!("✓ Verified: app owned by A\n");
    Ok(())
}

pub async fn test_owner_isolation(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 2: Owner isolation (B cannot touch A's app) ---");

    let app_id = format!("app-owner-{}", chrono::Utc::now().timestamp());
    let owner_a = "owner_a_pubkey";
    let owner_b = "owner_b_pubkey";

    let app_a = make_app(&app_id, owner_a, "App A");
    backend.save_app(&app_a).await?;
    println!("✓ App created by owner A");

    println!("Owner B attempts update...");
    let app_b = make_app(&app_id, owner_b, "Hijacked");
    backend.update_app(&app_b).await?;

    let after = backend.load_app(&app_id).await?.ok_or("App not found")?;
    if after.owner_dev_pubkey != owner_a {
        println!("✗ FAIL: Owner changed to B");
        return Err("Owner isolation violated".into());
    }
    println!("✓ Owner still A after update attempt\n");
    Ok(())
}
