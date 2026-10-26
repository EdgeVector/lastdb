//! Quick test to verify moto 5.x supports If-None-Match on PutObject.

use aws_sdk_s3::primitives::ByteStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_config::from_env()
        .endpoint_url("http://localhost:5000")
        .region("us-east-1")
        .load()
        .await;

    let client = aws_sdk_s3::Client::new(&config);

    // Create bucket
    println!("Creating bucket 'test-conditional'...");
    client
        .create_bucket()
        .bucket("test-conditional")
        .send()
        .await?;

    // First PUT without condition (should succeed)
    println!("\n1. PUT object without condition (should succeed)...");
    let result = client
        .put_object()
        .bucket("test-conditional")
        .key("test-key")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await;

    match result {
        Ok(_) => println!("   ✓ SUCCESS: Object created"),
        Err(e) => println!("   ✗ FAILED: {}", e),
    }

    // Second PUT with If-None-Match: * (should fail with 412)
    println!("\n2. PUT same object with If-None-Match: * (should fail with 412)...");
    let result = client
        .put_object()
        .bucket("test-conditional")
        .key("test-key")
        .body(ByteStream::from_static(b"world"))
        .if_none_match("*")
        .send()
        .await;

    match result {
        Ok(_) => println!("   ✗ UNEXPECTED SUCCESS: Should have failed with 412"),
        Err(e) => {
            println!("   Error details: {:?}", e);
            let err_str = format!("{:?}", e);
            if err_str.contains("412") || err_str.contains("PreconditionFailed") {
                println!("   ✓ SUCCESS: Got expected 412 Precondition Failed");
            } else {
                println!("   ✗ WRONG ERROR: {}", err_str);
            }
        }
    }

    Ok(())
}
