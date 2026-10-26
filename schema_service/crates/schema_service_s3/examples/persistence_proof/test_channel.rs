//! Tests for channel generation conflict detection.

use schema_service_core::app_release::{channel_key, ChannelRecord};
use schema_service_core::external_persistence::ExternalSchemaPersistence;
use schema_service_s3::S3BlobPersistence;

pub async fn test_channel_generation(
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

    println!("Attempting stale update (read gen=1, computed next=2, but stored is already 2)...");
    let stale = ChannelRecord {
        app_id: app_id.clone(),
        channel: channel.to_string(),
        release_id: "rel_stale".to_string(),
        generation: 2,
        updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };

    match backend.save_channel(&stale).await {
        Err(e) if e.to_string().contains("Generation conflict") => {
            println!("✓ Stale update rejected: {e}\n");
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
