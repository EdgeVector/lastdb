use fold_db::sync::SyncEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    let result = SyncEngine::pr354_recovery_drill(&args[1], std::path::Path::new(&args[2])).await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
