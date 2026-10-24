type Result = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result {
    // The unoptimized CI build needs room for the native factory's async frames.
    // This entry point does not measure latency or change product functions.
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(32 * 1024 * 1024)
                .build()?
                .block_on(run())
        })?
        .join()
        .map_err(|_| std::io::Error::other("drill runtime thread panicked"))?
}

async fn run() -> Result {
    let args: Vec<String> = std::env::args().collect();
    let fixed = args[3] == "fixed";
    let result = fold_db::pr369_drill::run(&args[1], std::path::Path::new(&args[2]), fixed).await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
