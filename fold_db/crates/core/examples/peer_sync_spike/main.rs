//! Opt-in operational command for the isolated logical peer protocol.
//! It uses explicit prototype roots, encrypted V2 files, and no daemon.

use fold_db::storage::disk_pack::peer_sync_spike::{Change, Replica};
use std::io::Write;
use std::path::Path;

mod files;
mod operations;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        return Err(usage().into());
    }
    let started = std::time::Instant::now();
    let result = run(&args).await;
    eprintln!(
        "command={} elapsed_ms={:.3}",
        args[0],
        started.elapsed().as_secs_f64() * 1000.0
    );
    result
}

async fn run(args: &[String]) -> Result<()> {
    match args[0].as_str() {
        "key" => {
            require(args, 2)?;
            files::create_key(Path::new(&args[1]))?;
            emit(&serde_json::json!({"created": true}))
        }
        "init" => {
            require(args, 3)?;
            let key = files::read_key(Path::new(&args[2]))?;
            let replica = Replica::open(Path::new(&args[1]), &key).await?;
            operations::status(&replica).await
        }
        command => {
            if args.len() < 2 {
                return Err(usage().into());
            }
            let mut replica = Replica::open_saved(Path::new(&args[1])).await?;
            run_open(&mut replica, command, args).await
        }
    }
}

async fn run_open(replica: &mut Replica, command: &str, args: &[String]) -> Result<()> {
    match command {
        "status" => {
            require(args, 2)?;
            operations::status(replica).await
        }
        "put" | "delete" => {
            require(args, if command == "put" { 5 } else { 4 })?;
            let change = Change {
                hash: args[2].clone(),
                range: args[3].clone(),
                value: (command == "put").then(|| args[4].clone()),
            };
            let entry = replica.author(&replica.writer()?, vec![change]).await?;
            emit(&entry)
        }
        "batch" => operations::batch(replica, args).await,
        "export" => operations::export(replica, args).await,
        "import" => operations::import(replica, args).await,
        "get" => {
            require(args, 4)?;
            emit(&replica.get(&args[2], &args[3]).await?)
        }
        "range" => {
            require(args, 5)?;
            let after = (args[3] != "-").then_some(args[3].as_str());
            emit(&replica.range(&args[2], after, args[4].parse()?).await?)
        }
        "dump" => operations::dump(replica, args).await,
        "snapshot" => {
            require(args, 3)?;
            emit(&replica.snapshot(Path::new(&args[2])).await?)
        }
        "adopt" => {
            require(args, 3)?;
            emit(&replica.adopt(Path::new(&args[2])).await?)
        }
        _ => Err(usage().into()),
    }
}

fn require(args: &[String], count: usize) -> Result<()> {
    if args.len() != count {
        return Err(usage().into());
    }
    Ok(())
}

fn emit(value: &impl serde::Serialize) -> Result<()> {
    serde_json::to_writer(std::io::stdout().lock(), value)?;
    std::io::stdout().write_all(b"\n")?;
    Ok(())
}

fn usage() -> &'static str {
    "peer_sync_spike: key KEYFILE | init ROOT KEYFILE | status ROOT | put ROOT HASH RANGE VALUE | \
     delete ROOT HASH RANGE | batch ROOT HASH START COUNT VALUE_BYTES | \
     export ROOT ORIGIN AFTER OUTFILE [LIMIT] | import ROOT INFILE | get ROOT HASH RANGE | \
     range ROOT HASH AFTER_OR_DASH LIMIT | dump ROOT HASH OUTFILE | snapshot ROOT OUTDIR | adopt ROOT SNAPSHOT"
}
