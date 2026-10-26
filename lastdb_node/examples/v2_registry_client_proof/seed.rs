//! S3 seeding and manipulation functions for the proof.

use aws_sdk_s3::primitives::ByteStream;
use serde_json::json;

/// Seeds the moto S3 bucket with test app and release data.
pub async fn seed_s3_data() -> Result<String, Box<dyn std::error::Error>> {
    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "proof-v2".to_string());
    let endpoint = std::env::var("S3_ENDPOINT").ok();
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Some(ep) = endpoint {
        s3_config_builder = s3_config_builder.endpoint_url(ep).force_path_style(true);
    }
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    let _ = s3_client.create_bucket().bucket(&bucket).send().await;

    let app = json!({
        "app_id": "proof-app",
        "owner_dev_pubkey": "owner_key",
        "metadata": {
            "display_name": "Proof App",
            "description": "Test",
            "homepage_url": "https://example.com",
            "icon_url": null
        },
        "version": "1.0.0",
        "registered_at": "2026-10-26T21:00:00Z",
        "tier": "Live"
    });

    s3_client
        .put_object()
        .bucket(&bucket)
        .key("apps/proof-app.json")
        .body(ByteStream::from(serde_json::to_vec(&app)?))
        .content_type("application/json")
        .send()
        .await?;

    let release_id = "aaaa1111222233334444555566667777888899990000aaaabbbbccccddddeeee";
    let release = json!({
        "release_id": release_id,
        "manifest": {
            "app_id": "proof-app",
            "app_uuid": "proof-uuid",
            "schemas": {},
            "source_commit": "abc123",
            "artifact_digest": "digest123",
            "artifact_url": "https://example.com/artifact",
            "artifact_signature": "sig123"
        },
        "publisher_dev_pubkey": "owner_key",
        "published_at": "2026-10-26T21:00:00Z"
    });

    s3_client
        .put_object()
        .bucket(&bucket)
        .key(format!("releases/{}.json", release_id))
        .body(ByteStream::from(serde_json::to_vec(&release)?))
        .content_type("application/json")
        .send()
        .await?;

    Ok(release_id.to_string())
}

/// Tampers with a stored compat row to test verification failure.
pub async fn tamper_stored_row(
    release_id: &str,
    contract_version: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "proof-v2".to_string());
    let endpoint = std::env::var("S3_ENDPOINT").ok();
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Some(ep) = endpoint {
        s3_config_builder = s3_config_builder.endpoint_url(ep).force_path_style(true);
    }
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    let key = format!("compat/{}/{}.json", release_id, contract_version);

    let obj = s3_client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await?;

    let body = obj.body.collect().await?;
    let mut row: serde_json::Value = serde_json::from_slice(&body.into_bytes())?;

    row["contract_version"] = json!("99.99.99");

    s3_client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(ByteStream::from(serde_json::to_vec(&row)?))
        .content_type("application/json")
        .send()
        .await?;

    Ok(())
}
