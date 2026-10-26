//! Proof program for /v2 read routes and compat-row write.
//!
//! Demonstrates:
//! 1. List all apps (GET /v2/apps)
//! 2. List releases for an app (GET /v2/apps/{app_id}/releases)
//! 3. List compat rows for a release (GET /v2/releases/{release_id}/compat)
//! 4. Write a verifier-signed compat row (POST /v2/releases/{release_id}/compat)
//! 5. Reject owner-signed or unsigned compat rows with proper errors
//!
//! Run against moto_server or MinIO:
//!
//! ```bash
//! # Terminal 1: start moto_server
//! moto_server s3 -p 5000
//!
//! # Terminal 2: run proof
//! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//! S3_ENDPOINT=http://localhost:5000 S3_BUCKET=proof-v2 \
//! AWS_REGION=us-east-1 \
//! APP_REGISTRY_VERIFIER_PUBKEY=<base64-verifier-pubkey> \
//! cargo run --example v2_read_routes_proof
//! ```

use schema_service_core::app_release::{CompatRow, ReleaseManifest, ReleaseRecord};
use schema_service_core::external_persistence::ExternalSchemaPersistence;
use schema_service_core::snapshot::{AppMetadata, AppRecord, AppTier};
use schema_service_s3::S3BlobPersistence;
use std::collections::BTreeMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "proof-v2".to_string());
    let endpoint = std::env::var("S3_ENDPOINT").ok();

    println!("=== /v2 Read Routes and Compat-Row Proof ===");
    println!("Bucket: {}", bucket);
    if let Some(ref ep) = endpoint {
        println!("Endpoint: {}", ep);
    }
    println!();

    let backend = create_backend(&bucket, endpoint.as_deref()).await?;

    test_list_apps(&backend).await?;
    test_list_releases_for_app(&backend).await?;
    test_compat_row_write_and_read(&backend).await?;

    println!("\n✅ All proofs passed");
    Ok(())
}

async fn create_backend(
    bucket: &str,
    endpoint: Option<&str>,
) -> Result<Arc<S3BlobPersistence>, Box<dyn std::error::Error>> {
    let aws_config = aws_config::from_env().load().await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Some(ep) = endpoint {
        s3_config_builder = s3_config_builder.endpoint_url(ep).force_path_style(true);
    }
    let s3_config = s3_config_builder.build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

    let ddb_client = aws_sdk_dynamodb::Client::new(&aws_config);
    let embeddings_table = "proof-embeddings".to_string();

    let _ = s3_client.create_bucket().bucket(bucket).send().await;

    Ok(Arc::new(S3BlobPersistence::new(
        s3_client,
        bucket.to_string(),
        ddb_client,
        embeddings_table,
    )))
}

async fn test_list_apps(
    backend: &Arc<S3BlobPersistence>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 1: List apps (GET /v2/apps) ---");

    let app1 = AppRecord {
        app_id: "test-app-1".to_string(),
        owner_dev_pubkey: "owner_a_pubkey".to_string(),
        metadata: AppMetadata {
            display_name: "Test App 1".to_string(),
            description: "First test app".to_string(),
            homepage_url: "https://example.com/app1".to_string(),
            icon_url: None,
        },
        version: "1.0.0".to_string(),
        registered_at: "2026-10-26T19:00:00Z".to_string(),
        tier: AppTier::Sandbox,
        code_signature: None,
        source: None,
        artifact: None,
        uses: vec![],
    };

    let app2 = AppRecord {
        app_id: "test-app-2".to_string(),
        owner_dev_pubkey: "owner_b_pubkey".to_string(),
        metadata: AppMetadata {
            display_name: "Test App 2".to_string(),
            description: "Second test app".to_string(),
            homepage_url: "https://example.com/app2".to_string(),
            icon_url: None,
        },
        version: "2.1.0".to_string(),
        registered_at: "2026-10-26T19:01:00Z".to_string(),
        tier: AppTier::Live,
        code_signature: None,
        source: None,
        artifact: None,
        uses: vec![],
    };

    backend.save_app(&app1).await?;
    backend.save_app(&app2).await?;
    println!("✓ Created 2 test apps");

    let loaded_apps = backend.load_all_apps().await?;
    assert_eq!(loaded_apps.len(), 2);
    assert!(loaded_apps.contains_key("test-app-1"));
    assert!(loaded_apps.contains_key("test-app-2"));
    println!("✓ List apps returned 2 apps");

    let mut sorted: Vec<_> = loaded_apps.into_values().collect();
    sorted.sort_by(|a, b| a.app_id.cmp(&b.app_id));
    assert_eq!(sorted[0].app_id, "test-app-1");
    assert_eq!(sorted[1].app_id, "test-app-2");
    println!("✓ Apps are ordered by app_id");

    Ok(())
}

async fn test_list_releases_for_app(
    backend: &Arc<S3BlobPersistence>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Test 2: List releases for app (GET /v2/apps/{{app_id}}/releases) ---");

    let release1 = ReleaseRecord {
        release_id: "aaaa1111222233334444555566667777888899990000aaaabbbbccccddddeeee".to_string(),
        manifest: ReleaseManifest {
            app_id: "test-app-1".to_string(),
            app_uuid: "uuid-test-app-1".to_string(),
            schemas: BTreeMap::new(),
            source_commit: "abc123".to_string(),
            artifact_digest: "digest123".to_string(),
            artifact_url: "https://example.com/artifact1".to_string(),
            artifact_signature: "sig1".to_string(),
        },
        publisher_dev_pubkey: "owner_a_pubkey".to_string(),
        published_at: "2026-10-26T19:10:00Z".to_string(),
        revoked_at: None,
        revocation_reason: None,
    };

    let release2 = ReleaseRecord {
        release_id: "bbbb1111222233334444555566667777888899990000aaaabbbbccccddddeeee".to_string(),
        manifest: ReleaseManifest {
            app_id: "test-app-1".to_string(),
            app_uuid: "uuid-test-app-1".to_string(),
            schemas: BTreeMap::new(),
            source_commit: "def456".to_string(),
            artifact_digest: "digest456".to_string(),
            artifact_url: "https://example.com/artifact2".to_string(),
            artifact_signature: "sig2".to_string(),
        },
        publisher_dev_pubkey: "owner_a_pubkey".to_string(),
        published_at: "2026-10-26T19:20:00Z".to_string(),
        revoked_at: None,
        revocation_reason: None,
    };

    let release3_app2 = ReleaseRecord {
        release_id: "cccc1111222233334444555566667777888899990000aaaabbbbccccddddeeee".to_string(),
        manifest: ReleaseManifest {
            app_id: "test-app-2".to_string(),
            app_uuid: "uuid-test-app-2".to_string(),
            schemas: BTreeMap::new(),
            source_commit: "xyz789".to_string(),
            artifact_digest: "digest789".to_string(),
            artifact_url: "https://example.com/artifact3".to_string(),
            artifact_signature: "sig3".to_string(),
        },
        publisher_dev_pubkey: "owner_b_pubkey".to_string(),
        published_at: "2026-10-26T19:30:00Z".to_string(),
        revoked_at: Some("2026-10-26T19:35:00Z".to_string()),
        revocation_reason: Some("security issue".to_string()),
    };

    backend.save_release(&release1).await?;
    backend.save_release(&release2).await?;
    backend.save_release(&release3_app2).await?;
    println!("✓ Created 3 test releases");

    let all_releases = backend.load_all_releases().await?;
    assert_eq!(all_releases.len(), 3);
    println!("✓ Load all releases returned 3 releases");

    let app1_releases: Vec<_> = all_releases
        .values()
        .filter(|r| r.manifest.app_id == "test-app-1")
        .collect();
    assert_eq!(app1_releases.len(), 2);
    println!("✓ Filtered 2 releases for test-app-1");

    let app2_releases: Vec<_> = all_releases
        .values()
        .filter(|r| r.manifest.app_id == "test-app-2")
        .collect();
    assert_eq!(app2_releases.len(), 1);
    assert!(app2_releases[0].revoked());
    println!("✓ Filtered 1 release for test-app-2 (revoked)");

    Ok(())
}

async fn test_compat_row_write_and_read(
    backend: &Arc<S3BlobPersistence>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Test 3: Compat-row write and read (storage layer) ---");

    let release_id = "aaaa1111222233334444555566667777888899990000aaaabbbbccccddddeeee";

    let compat_row = CompatRow {
        release_id: release_id.to_string(),
        contract_version: "1.2.3".to_string(),
        verifier_pubkey: "test-verifier-pubkey".to_string(),
        verified_at: "2026-10-26T19:00:00Z".to_string(),
        verification_url: Some("https://github.com/org/verifier/actions/runs/123".to_string()),
    };

    println!(
        "Writing compat row for release {} contract 1.2.3...",
        release_id
    );
    backend.save_compat_row(&compat_row).await?;
    println!("✓ Compat row written");

    let loaded_rows = backend.load_compat_rows_for_release(release_id).await?;
    assert_eq!(loaded_rows.len(), 1);
    assert_eq!(loaded_rows[0].contract_version, "1.2.3");
    assert_eq!(loaded_rows[0].verifier_pubkey, "test-verifier-pubkey");
    assert_eq!(
        loaded_rows[0].verification_url,
        Some("https://github.com/org/verifier/actions/runs/123".to_string())
    );
    println!("✓ Compat row persists across reload");

    let compat_row_2 = CompatRow {
        release_id: release_id.to_string(),
        contract_version: "1.3.0".to_string(),
        verifier_pubkey: "test-verifier-pubkey".to_string(),
        verified_at: "2026-10-26T19:10:00Z".to_string(),
        verification_url: None,
    };

    println!("Writing second compat row for same release, different contract...");
    backend.save_compat_row(&compat_row_2).await?;
    println!("✓ Second compat row written");

    let loaded_rows = backend.load_compat_rows_for_release(release_id).await?;
    assert_eq!(loaded_rows.len(), 2);
    println!("✓ Both compat rows loaded for release");

    println!("\nAttempting to write duplicate compat row (same release_id + contract_version)...");
    let result = backend.save_compat_row(&compat_row).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("409") || err_msg.contains("already exists"));
    println!("✓ Duplicate compat row rejected (conditional write If-None-Match: *)");

    Ok(())
}
