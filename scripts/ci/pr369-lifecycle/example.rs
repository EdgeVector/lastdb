#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    let fixed = args[3] == "fixed";
    let result = fold_db::pr369_drill::run(&args[1], std::path::Path::new(&args[2]), fixed).await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
