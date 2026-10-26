//! Proof of per-record S3 persistence with conditional writes and owner binding.
//!
//! Run with:
//! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//! S3_ENDPOINT=http://localhost:5000 S3_BUCKET=test-bucket \
//! cargo run --example persistence_proof

mod helpers;
mod test_app_lifecycle;
mod test_channel;
mod test_domain_checks;
mod test_legacy;
mod test_release;

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client as DdbClient;
use aws_sdk_s3::Client as S3Client;
use schema_service_s3::S3BlobPersistence;

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

    let mut aws_config_loader = aws_config::defaults(BehaviorVersion::latest()).region("us-east-1");
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

    test_app_lifecycle::test_concurrent_create(&backend).await?;
    test_app_lifecycle::test_owner_isolation(&backend).await?;
    test_release::test_release_persistence(&backend).await?;
    test_channel::test_channel_generation(&backend).await?;
    test_legacy::test_legacy_app_takeover(&backend).await?;
    test_release::test_revocation_preservation(&backend).await?;
    test_domain_checks::test_domain_layer_owner_checks(&backend).await?;

    println!("\n✅ All proofs passed");
    Ok(())
}
