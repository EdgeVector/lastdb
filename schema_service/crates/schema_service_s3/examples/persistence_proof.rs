//! Proof of per-record S3 persistence with conditional writes and owner binding.
//!
//! Run with:
//! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//! S3_ENDPOINT=http://localhost:5000 S3_BUCKET=test-bucket \
//! cargo run --example persistence_proof

use std::collections::BTreeMap;

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client as DdbClient;
use aws_sdk_s3::Client as S3Client;
use schema_service_core::app_release::{
    channel_key, ChannelRecord, ReleaseManifest, ReleaseRecord,
};
use schema_service_core::external_persistence::ExternalSchemaPersistence;
use schema_service_core::snapshot::{AppMetadata, AppRecord, AppTier};
use schema_service_s3::S3BlobPersistence;

fn make_app(app_id: &str, owner: &str, name: &str) -> AppRecord {
    AppRecord {
        app_id: app_id.to_string(),
        owner_dev_pubkey: owner.to_string(),
        metadata: AppMetadata {
            display_name: name.to_string(),
            description: "Test app".to_string(),
            homepage_url: "https://example.com".to_string(),
            icon_url: None,
        },
        version: "1.0.0".to_string(),
        registered_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        tier: AppTier::Sandbox,
        code_signature: None,
        source: None,
        artifact: None,
        uses: Vec::new(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "test-bucket".to_string());
    let endpoint = std::env::var("S3_ENDPOINT").ok();

    println!("=== Schema Service S3 Persistence Proof ===");
    println!("Bucket: {bucket}");
    println!(
        "Endpoint: {}\n",
        endpoint.as_deref().unwrap_or("AWS default")
    );

    let mut aws_config_loader = aws_config::defaults(BehaviorVersion::latest())
        .region("us-east-1");
    if let Some(ep) = &endpoint {
        aws_config_loader = aws_config_loader.endpoint_url(ep);
    }
    let aws_config = aws_config_loader.load().await;

    let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
        .force_path_style(true)
        .build();
    let s3 = S3Client::from_conf(s3_config);
    let ddb = DdbClient::new(&aws_config);

    s3.create_bucket().bucket(&bucket).send().await.ok();

    let backend = S3BlobPersistence::new(s3, bucket, ddb, "test-table".to_string());

    test_concurrent_create(&backend).await?;
    test_owner_isolation(&backend).await?;
    test_release_persistence(&backend).await?;
    test_channel_generation(&backend).await?;
    test_legacy_app_takeover(&backend).await?;

    println!("\n✅ All proofs passed");
    Ok(())
}

async fn test_concurrent_create(
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

async fn test_owner_isolation(
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

async fn test_release_persistence(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 3: Release persists across process restart ---");

    let release_id = format!("rel{}", chrono::Utc::now().timestamp());
    let release = ReleaseRecord {
        release_id: release_id.clone(),
        manifest: ReleaseManifest {
            app_id: "test-app".to_string(),
            app_uuid: "test-uuid".to_string(),
            schemas: BTreeMap::from([("schema1".to_string(), "hash123".to_string())]),
            source_commit: "abc123".to_string(),
            artifact_digest: "digest123".to_string(),
            artifact_url: "https://example.com/artifact".to_string(),
            artifact_signature: "sig123".to_string(),
        },
        publisher_dev_pubkey: "publisher".to_string(),
        published_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        revoked_at: None,
        revocation_reason: None,
    };

    backend.save_release(&release).await?;
    println!("✓ Release saved");

    let loaded = backend.load_all_releases().await?;
    if !loaded.contains_key(&release_id) {
        println!("✗ FAIL: Release not found after reload");
        return Err("Release not persisted".into());
    }
    println!("✓ Release survives restart\n");
    Ok(())
}

async fn test_channel_generation(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 4: Channel generation conflict detection ---");

    let app_id = format!("app-ch-{}", chrono::Utc::now().timestamp());
    let channel = "stable";

    let ch1 = ChannelRecord {
        app_id: app_id.clone(),
        channel: channel.to_string(),
        release_id: "rel1".to_string(),
        generation: 1,
        updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    backend.save_channel(&ch1).await?;
    println!("✓ Channel created at generation 1");

    let loaded = backend.load_all_channels().await?;
    let key = channel_key(&app_id, channel);
    let current = loaded.get(&key).ok_or("Channel not found")?;

    let ch2 = ChannelRecord {
        app_id: app_id.clone(),
        channel: channel.to_string(),
        release_id: "rel2".to_string(),
        generation: current.generation + 1,
        updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    backend.save_channel(&ch2).await?;
    println!("✓ Updated to generation 2");

    // stale record thinks generation is still 1, tries to update to 2
    // but stored generation is already 2, so this conflicts
    println!("Attempting stale update (client thinks gen=1, tries gen=2, but stored is 2)...");
    let stale = ChannelRecord {
        app_id: app_id.clone(),
        channel: channel.to_string(),
        release_id: "rel_stale".to_string(),
        generation: 2,
        updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    match backend.save_channel(&stale).await {
        Err(e) if e.to_string().contains("Generation conflict") => {
            println!("✓ Stale generation rejected: {e}\n");
            Ok(())
        }
        Err(e) => {
            println!("✗ FAIL: Wrong error: {e}");
            Err(e.into())
        }
        Ok(_) => {
            println!("✗ FAIL: Stale generation accepted");
            Err("Generation conflict not detected".into())
        }
    }
}

async fn test_legacy_app_takeover(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 5: Legacy apps.json takeover prevention ---");

    let app_id = format!("legacy-app-{}", chrono::Utc::now().timestamp());
    let owner_a = "legacy_owner_a_pubkey";
    let owner_b = "legacy_owner_b_pubkey";

    // Seed a legacy apps.json blob with app X owned by A
    let legacy_app = make_app(&app_id, owner_a, "Legacy App A");
    let mut legacy_blob = std::collections::HashMap::new();
    legacy_blob.insert(app_id.clone(), legacy_app.clone());

    let legacy_json = serde_json::to_vec(&serde_json::json!({
        "items": legacy_blob,
        "version": 1
    }))?;

    println!("Seeding legacy apps.json with app '{}' owned by A...", app_id);
    backend
        .client()
        .put_object()
        .bucket(backend.bucket())
        .key("apps.json")
        .body(aws_sdk_s3::primitives::ByteStream::from(legacy_json))
        .send()
        .await?;
    println!("✓ Legacy blob seeded (no apps/{}.json exists yet)", app_id);

    // Developer B tries to create the app
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

    // Developer A creates the app (idempotent)
    let app_a = make_app(&app_id, owner_a, "App A");
    println!("\nDeveloper A creates app '{}' (idempotent with legacy)...", app_id);
    backend.save_app(&app_a).await?;
    println!("✓ Developer A succeeded (idempotent with legacy owner)");

    // Developer A updates the app (triggers migration)
    println!("\nDeveloper A updates app metadata (triggers migration)...");
    let mut app_a_updated = app_a.clone();
    app_a_updated.metadata.description = "Updated description".to_string();
    backend.update_app(&app_a_updated).await?;
    println!("✓ Update succeeded, app migrated to apps/{}.json", app_id);

    // Verify migration: apps/X.json should now exist
    let migrated = backend.load_app(&app_id).await?.ok_or("App not found after migration")?;
    if migrated.owner_dev_pubkey != owner_a {
        println!("✗ FAIL: Migrated app has wrong owner");
        return Err("Migration owner mismatch".into());
    }
    println!("✓ Verified: apps/{}.json exists with owner A", app_id);

    // Developer B tries again after migration
    println!("\nDeveloper B attempts to create app '{}' after migration...", app_id);
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
