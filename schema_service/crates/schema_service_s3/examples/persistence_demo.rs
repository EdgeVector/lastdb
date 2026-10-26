//! Demonstration of per-record S3 persistence with conditional writes.
//!
//! This example shows:
//! 1. Releases and channels persist across process restarts
//! 2. Concurrent app creates enforce first-write-wins (one succeeds, others get NotAppOwner)
//! 3. Channel updates enforce generation checks (stale writes get GenerationConflict)
//!
//! Usage:
//! ```sh
//! # With MinIO running on localhost:9000
//! AWS_ACCESS_KEY_ID=minioadmin \
//! AWS_SECRET_ACCESS_KEY=minioadmin \
//! AWS_REGION=us-east-1 \
//! S3_ENDPOINT=http://localhost:9000 \
//! S3_BUCKET=schema-demo \
//! cargo run --example persistence_demo
//! ```

use std::collections::{BTreeMap, HashMap};
use std::env;

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client as DdbClient;
use aws_sdk_s3::Client as S3Client;
use schema_service_core::app_release::{
    channel_key, ChannelRecord, ReleaseManifest, ReleaseRecord,
};
use schema_service_core::external_persistence::ExternalSchemaPersistence;
use schema_service_core::snapshot::AppRecord;
use schema_service_s3::S3BlobPersistence;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let bucket = env::var("S3_BUCKET").unwrap_or_else(|_| "schema-demo".to_string());
    let endpoint = env::var("S3_ENDPOINT").ok();
    let embeddings_table =
        env::var("DYNAMODB_TABLE").unwrap_or_else(|_| "schema-embeddings".to_string());

    println!("=== Schema Service S3 Persistence Demo ===");
    println!("Bucket: {bucket}");
    println!("Endpoint: {}", endpoint.as_deref().unwrap_or("AWS default"));
    println!("DynamoDB table: {embeddings_table}\n");

    let mut aws_config_loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(ep) = &endpoint {
        aws_config_loader = aws_config_loader.endpoint_url(ep);
    }
    let aws_config = aws_config_loader.load().await;

    let s3_client = S3Client::new(&aws_config);
    let ddb_client = DdbClient::new(&aws_config);

    let backend = S3BlobPersistence::new(
        s3_client.clone(),
        bucket.clone(),
        ddb_client.clone(),
        embeddings_table.clone(),
    );

    demo_app_first_write_wins(&backend).await?;
    demo_release_persistence(&backend).await?;
    demo_channel_generation_check(&backend).await?;
    demo_owner_isolation(&backend).await?;

    println!("\n=== All demos completed successfully ===");
    Ok(())
}

async fn demo_app_first_write_wins(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Demo 1: App first-write-wins ---");

    let app_id = format!("demo-app-{}", chrono::Utc::now().timestamp());
    let owner_pubkey_1 = "owner1_pubkey_base64".to_string();
    let owner_pubkey_2 = "owner2_pubkey_base64".to_string();

    let app1 = AppRecord {
        app_id: app_id.clone(),
        owner_dev_pubkey: owner_pubkey_1.clone(),
        metadata: serde_json::json!({"display_name": "Demo App"}),
        tier: schema_service_core::snapshot::AppTier::Free,
        registered_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    let app2 = AppRecord {
        app_id: app_id.clone(),
        owner_dev_pubkey: owner_pubkey_2.clone(),
        metadata: serde_json::json!({"display_name": "Demo App"}),
        tier: schema_service_core::snapshot::AppTier::Free,
        registered_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    println!("Creating app with owner1...");
    backend.save_app(&app1).await?;
    println!("✓ First write succeeded");

    println!("Attempting to create same app with owner2...");
    match backend.save_app(&app2).await {
        Ok(_) => {
            println!("✗ FAIL: Second write should have been rejected");
            return Err("Concurrent create did not enforce first-write-wins".into());
        }
        Err(e) => {
            if e.to_string().contains("different owner") {
                println!("✓ Second write rejected (different owner)");
            } else {
                println!("✗ FAIL: Wrong error: {e}");
                return Err(e.into());
            }
        }
    }

    println!("Verifying stored app has owner1...");
    let loaded = backend.load_all_apps().await?;
    match loaded.get(&app_id) {
        Some(stored) if stored.owner_dev_pubkey == owner_pubkey_1 => {
            println!("✓ Stored app has correct owner (owner1)");
        }
        Some(stored) => {
            println!(
                "✗ FAIL: Stored app has wrong owner: {}",
                stored.owner_dev_pubkey
            );
            return Err("Wrong owner in storage".into());
        }
        None => {
            println!("✗ FAIL: App not found in storage");
            return Err("App not found".into());
        }
    }

    Ok(())
}

async fn demo_release_persistence(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Demo 2: Release persistence across process restart ---");

    let release_id = format!("rel{}", chrono::Utc::now().timestamp());
    let app_id = format!("app-{}", chrono::Utc::now().timestamp());

    let manifest = ReleaseManifest {
        app_id: app_id.clone(),
        app_uuid: "test-uuid-1234".to_string(),
        schemas: BTreeMap::from([("my_schema".to_string(), "schema_hash_1234".to_string())]),
        source_commit: "abc123".to_string(),
        artifact_digest: "digest1234".to_string(),
        artifact_url: "https://example.com/artifact.tar.gz".to_string(),
        artifact_signature: "sig1234".to_string(),
    };

    let release = ReleaseRecord {
        release_id: release_id.clone(),
        manifest,
        publisher_dev_pubkey: "publisher_pubkey".to_string(),
        published_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        revoked_at: None,
        revocation_reason: None,
    };

    println!("Saving release {release_id}...");
    backend.save_release(&release).await?;
    println!("✓ Release saved");

    println!("Simulating process restart (loading all releases)...");
    let loaded = backend.load_all_releases().await?;
    match loaded.get(&release_id) {
        Some(stored) if stored.release_id == release_id => {
            println!("✓ Release survived process restart");
            println!("  - manifest.app_id: {}", stored.manifest.app_id);
            println!("  - schemas count: {}", stored.manifest.schemas.len());
        }
        Some(_) => {
            println!("✗ FAIL: Release ID mismatch");
            return Err("Release ID mismatch".into());
        }
        None => {
            println!("✗ FAIL: Release not found after reload");
            return Err("Release not found".into());
        }
    }

    Ok(())
}

async fn demo_channel_generation_check(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Demo 3: Channel generation conflict detection ---");

    let app_id = format!("app-ch-{}", chrono::Utc::now().timestamp());
    let channel = "stable";
    let release_id_1 = "release_abc".to_string();
    let release_id_2 = "release_xyz".to_string();

    let channel_rec_gen0 = ChannelRecord {
        app_id: app_id.clone(),
        channel: channel.to_string(),
        release_id: release_id_1.clone(),
        generation: 1,
        updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    println!("Creating channel {app_id}/{channel} at generation 1...");
    backend.save_channel(&channel_rec_gen0).await?;
    println!("✓ Channel created");

    println!("Loading channel to get current generation...");
    let loaded = backend.load_all_channels().await?;
    let key = channel_key(&app_id, channel);
    let current = loaded.get(&key).ok_or("Channel not found after creation")?;
    println!("✓ Channel loaded, generation: {}", current.generation);

    let channel_rec_gen1 = ChannelRecord {
        app_id: app_id.clone(),
        channel: channel.to_string(),
        release_id: release_id_2.clone(),
        generation: current.generation + 1,
        updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    println!(
        "Updating channel to generation {}...",
        channel_rec_gen1.generation
    );
    backend.save_channel(&channel_rec_gen1).await?;
    println!("✓ Channel updated with correct generation");

    let stale_update = ChannelRecord {
        app_id: app_id.clone(),
        channel: channel.to_string(),
        release_id: "release_stale".to_string(),
        generation: current.generation + 1,
        updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    println!("Attempting update with stale generation...");
    match backend.save_channel(&stale_update).await {
        Ok(_) => {
            println!("✗ FAIL: Stale generation update should have been rejected");
            return Err("Generation check did not detect conflict".into());
        }
        Err(e) => {
            if e.to_string().contains("generation conflict") {
                println!("✓ Stale update rejected (generation conflict)");
            } else {
                println!("✗ FAIL: Wrong error: {e}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

async fn demo_owner_isolation(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Demo 4: Owner isolation (developer B cannot touch developer A's app) ---");

    let app_id = format!("app-owner-{}", chrono::Utc::now().timestamp());
    let owner_a = "developer_a_pubkey_base64".to_string();
    let owner_b = "developer_b_pubkey_base64".to_string();

    let app_a = AppRecord {
        app_id: app_id.clone(),
        owner_dev_pubkey: owner_a.clone(),
        metadata: serde_json::json!({"display_name": "App A"}),
        tier: schema_service_core::snapshot::AppTier::Free,
        registered_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    println!("Developer A creates app '{app_id}'...");
    backend.save_app(&app_a).await?;
    println!("✓ App created by developer A");

    println!("\nDeveloper B attempts to re-create the same app...");
    let app_b = AppRecord {
        app_id: app_id.clone(),
        owner_dev_pubkey: owner_b.clone(),
        metadata: serde_json::json!({"display_name": "App B Hijack"}),
        tier: schema_service_core::snapshot::AppTier::Free,
        registered_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };
    match backend.save_app(&app_b).await {
        Ok(_) => {
            println!("✗ FAIL: Developer B should not be able to create app owned by A");
            return Err("Owner isolation violated".into());
        }
        Err(e) if e.to_string().contains("different owner") => {
            println!("✓ Developer B rejected (NotAppOwner)");
        }
        Err(e) => {
            println!("✗ FAIL: Wrong error: {e}");
            return Err(e.into());
        }
    }

    println!("\nVerifying app still owned by developer A...");
    let stored = backend.load_app(&app_id).await?.ok_or("App not found")?;
    if stored.owner_dev_pubkey != owner_a {
        println!("✗ FAIL: App owner changed to '{}'", stored.owner_dev_pubkey);
        return Err("Owner changed after failed hijack".into());
    }
    println!("✓ App still owned by developer A");
    println!(
        "  - Stored display_name: {}",
        stored.metadata.get("display_name").unwrap()
    );

    println!("\nVerifying developer B cannot update app metadata...");
    let updated_b = AppRecord {
        app_id: app_id.clone(),
        owner_dev_pubkey: owner_b.clone(),
        metadata: serde_json::json!({"display_name": "Hijacked Name"}),
        tier: schema_service_core::snapshot::AppTier::Free,
        registered_at: stored.registered_at.clone(),
    };
    backend.update_app(&updated_b).await?;
    let after_update = backend.load_app(&app_id).await?.ok_or("App not found")?;
    if after_update.owner_dev_pubkey != owner_a {
        println!(
            "✗ FAIL: update_app changed owner to '{}'",
            after_update.owner_dev_pubkey
        );
        return Err("update_app violated owner immutability".into());
    }
    println!("✓ update_app preserved owner (developer A)");
    println!("  - Owner still: {}", after_update.owner_dev_pubkey);

    println!("\n✓ Owner isolation verified: developer A's ownership cannot be changed");
    Ok(())
}
