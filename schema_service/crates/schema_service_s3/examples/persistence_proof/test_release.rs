//! Tests for release persistence and revocation.

use schema_service_core::app_release::{ReleaseManifest, ReleaseRecord};
use schema_service_core::external_persistence::ExternalSchemaPersistence;
use schema_service_s3::S3BlobPersistence;
use std::collections::BTreeMap;

pub async fn test_release_persistence(
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

pub async fn test_revocation_preservation(
    backend: &S3BlobPersistence,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 6: Revocation preservation on delayed publish ---");

    let release_id = format!("rev{}", chrono::Utc::now().timestamp());
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
    println!("✓ Release published");

    let mut revoked = release.clone();
    revoked.revoked_at =
        Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    revoked.revocation_reason = Some("Security issue".to_string());
    backend.save_release(&revoked).await?;
    println!("✓ Release revoked");

    let loaded = backend
        .load_all_releases()
        .await?
        .get(&release_id)
        .cloned()
        .ok_or("Release not found")?;
    if loaded.revoked_at.is_none() {
        println!("✗ FAIL: Revocation not persisted");
        return Err("Revocation not saved".into());
    }
    println!("✓ Verified: revoked_at and revocation_reason stored");

    println!("Attempting delayed publish (simulating slow initial save after revocation)...");
    let delayed_publish = release.clone();
    backend.save_release(&delayed_publish).await?;
    println!("✓ Delayed publish completed");

    let final_state = backend
        .load_all_releases()
        .await?
        .get(&release_id)
        .cloned()
        .ok_or("Release not found")?;
    if final_state.revoked_at.is_none() {
        println!("✗ FAIL: Revocation was overwritten (revoked=false clobbered revoked=true)");
        return Err("Revocation not preserved".into());
    }
    println!("✓ Verified: revocation preserved (revoked=true not overwritten)\n");
    Ok(())
}
