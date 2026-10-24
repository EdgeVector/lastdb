//! Owner-only native record transport. Stored bodies never enter diagnostic output.

use laststore::disk_pack::home::{transfer, DiskHome};
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("native transfer failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.as_slice() == ["--help"] || args.as_slice() == ["-h"] {
        println!("usage: lastdb-native-transfer <export|import|verify> --home PHYSICAL_HOME");
        println!("export writes an opaque record stream to stdout; import reads stdin");
        println!("import requires a nonexistent target; interrupted transfers need a fresh target");
        println!("the caller must stop the source cleanly and retain its identity and keys");
        println!("this transports compatible native record codecs, not arbitrary older formats");
        return Ok(());
    }
    if args.as_slice() == ["--version"] {
        println!("lastdb-native-transfer {}", laststore::VERSION);
        return Ok(());
    }
    if args.len() != 3 || args[1] != "--home" {
        return Err("explicit command and physical home required".into());
    }
    let home = PathBuf::from(&args[2]);
    let receipt = match args[0].as_str() {
        "export" => transfer::export(
            &DiskHome::open(&home)?,
            &mut BufWriter::new(std::io::stdout().lock()),
        )?,
        "import" => transfer::import(&home, &mut BufReader::new(std::io::stdin().lock()))?,
        "verify" => transfer::export(&DiskHome::open(&home)?, &mut std::io::sink())?,
        _ => return Err("unknown native transfer command".into()),
    };
    // stdout belongs exclusively to the binary export stream.
    eprintln!("{}", serde_json::to_string(&receipt)?);
    Ok(())
}
