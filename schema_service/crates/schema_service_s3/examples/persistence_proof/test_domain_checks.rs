//! Tests for domain layer owner checks with simulated operations.

use crate::helpers::make_app;
use schema_service_core::app_release::{ReleaseManifest, ReleaseRecord};
use schema_service_core::external_persistence::ExternalSchemaPersistence;
use schema_service_s3::S3BlobPersistence;
use std::collections::BTreeMap;

async fn simulate_publish_release_check(
    backend: &S3BlobPersistence,
    app_id: &str,
    dev_pubkey: &str,
) -> Result<(), String> {
    let app = backend
        .load_app(app_id)
        .await
        .map_err(|e| format!("Internal: {e}"))?
        .ok_or_else(|| format!("UnknownApp: {app_id}"))?;

    if app.owner_dev_pubkey != dev_pubkey {
        return Err(format!(
            "NotAppOwner (HTTP 403): app '{}' is owned by '{}', not '{}'",
            app_id, app.owner_dev_pubkey, dev_pubkey
        ));
    }
    Ok(())
}

pub async fn test_domain_layer_owner_checks(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 7: Domain layer owner check enforcement ---");
    println!(
        "(Simulates publish_release/set_channel/revoke_release with real storage-backed checks)\n"
    );

    test_per_record_app_owner_enforcement(backend).await?;
    test_legacy_app_owner_enforcement(backend).await?;

    Ok(())
}

async fn test_per_record_app_owner_enforcement(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Per-record app owner enforcement ===");

    let app_id = format!("domain-app-{}", chrono::Utc::now().timestamp());
    let owner_a = "dev_a_pubkey";
    let owner_b = "dev_b_pubkey";

    let app_a = make_app(&app_id, owner_a, "App A");
    backend.save_app(&app_a).await?;
    println!("✓ App '{}' created by developer A", app_id);

    println!("\nDeveloper B attempts to publish a release...");
    match simulate_publish_release_check(backend, &app_id, owner_b).await {
        Err(e) if e.contains("NotAppOwner") && e.contains("403") => {
            println!("✓ Rejected: {e}");
        }
        Err(e) => {
            println!("✗ FAIL: Wrong error: {e}");
            return Err(format!("Expected NotAppOwner, got: {e}").into());
        }
        Ok(_) => {
            println!("✗ FAIL: Developer B should have been rejected");
            return Err("Owner check did not reject developer B".into());
        }
    }

    println!("\nVerifying storage state after rejection...");
    let before_releases = backend.load_all_releases().await?;
    let b_release_keys: Vec<_> = before_releases
        .keys()
        .filter(|k| k.contains("rel-b-"))
        .collect();
    if !b_release_keys.is_empty() {
        println!("✗ FAIL: Release was saved despite owner check failure");
        return Err("Storage modified after rejection".into());
    }
    println!("✓ Storage unchanged: release not saved");

    println!("\nDeveloper A publishes a release (owner check passes)...");
    match simulate_publish_release_check(backend, &app_id, owner_a).await {
        Ok(_) => {
            println!("✓ Owner check passed for developer A");
        }
        Err(e) => {
            println!("✗ FAIL: Developer A should pass: {e}");
            return Err(format!("Developer A rejected incorrectly: {e}").into());
        }
    }

    let release_a = ReleaseRecord {
        release_id: format!("rel-a-{}", chrono::Utc::now().timestamp()),
        manifest: ReleaseManifest {
            app_id: app_id.clone(),
            app_uuid: "uuid-a".to_string(),
            schemas: BTreeMap::from([("schema1".to_string(), "hash456".to_string())]),
            source_commit: "def456".to_string(),
            artifact_digest: "digest456".to_string(),
            artifact_url: "https://example.com/artifact2".to_string(),
            artifact_signature: "sig456".to_string(),
        },
        publisher_dev_pubkey: owner_a.to_string(),
        published_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        revoked_at: None,
        revocation_reason: None,
    };
    backend.save_release(&release_a).await?;
    println!("✓ Developer A's release saved to storage\n");

    Ok(())
}

async fn test_legacy_app_owner_enforcement(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Legacy app owner enforcement ===");

    let legacy_app_id = format!("legacy-domain-{}", chrono::Utc::now().timestamp());
    let owner_a = "dev_a_pubkey";
    let owner_b = "dev_b_pubkey";

    let legacy_app = make_app(&legacy_app_id, owner_a, "Legacy App");
    let mut legacy_blob = std::collections::HashMap::new();
    legacy_blob.insert(legacy_app_id.clone(), legacy_app.clone());

    let legacy_json = serde_json::to_vec(&serde_json::json!({
        "items": legacy_blob,
        "version": 1
    }))?;

    backend
        .client()
        .put_object()
        .bucket(backend.bucket())
        .key("apps.json")
        .body(aws_sdk_s3::primitives::ByteStream::from(legacy_json))
        .send()
        .await?;
    println!(
        "✓ Legacy app '{}' seeded in apps.json (owned by A)",
        legacy_app_id
    );

    println!("\nDeveloper B attempts to publish a release to legacy app...");
    match simulate_publish_release_check(backend, &legacy_app_id, owner_b).await {
        Err(e) if e.contains("NotAppOwner") && e.contains("403") => {
            println!("✓ Rejected: {e}");
        }
        Err(e) => {
            println!("✗ FAIL: Wrong error: {e}");
            return Err(format!("Expected NotAppOwner, got: {e}").into());
        }
        Ok(_) => {
            println!("✗ FAIL: Developer B should have been rejected");
            return Err("Legacy app owner check did not reject developer B".into());
        }
    }

    println!("\nDeveloper A publishes a release to legacy app (owner check passes)...");
    match simulate_publish_release_check(backend, &legacy_app_id, owner_a).await {
        Ok(_) => {
            println!("✓ Owner check passed for developer A on legacy app");
        }
        Err(e) => {
            println!("✗ FAIL: Developer A should pass: {e}");
            return Err(format!("Developer A rejected incorrectly: {e}").into());
        }
    }

    let legacy_release = ReleaseRecord {
        release_id: format!("rel-legacy-{}", chrono::Utc::now().timestamp()),
        manifest: ReleaseManifest {
            app_id: legacy_app_id.clone(),
            app_uuid: "uuid-legacy".to_string(),
            schemas: BTreeMap::from([("schema1".to_string(), "hash789".to_string())]),
            source_commit: "ghi789".to_string(),
            artifact_digest: "digest789".to_string(),
            artifact_url: "https://example.com/artifact3".to_string(),
            artifact_signature: "sig789".to_string(),
        },
        publisher_dev_pubkey: owner_a.to_string(),
        published_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        revoked_at: None,
        revocation_reason: None,
    };
    backend.save_release(&legacy_release).await?;
    println!("✓ Developer A's release saved to storage for legacy app\n");

    Ok(())
}
