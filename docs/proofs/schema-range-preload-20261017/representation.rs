//! Analysis-only measurement of synthetic ordered key representations.
//! This program does not link LastDB and does not open a database home.
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::hint::black_box;
use std::io::{self, BufWriter, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::time::{Duration, Instant};

struct Case {
    count: usize,
    characters: usize,
    text: bool,
}

impl Case {
    fn from_args(args: &[String]) -> io::Result<Self> {
        Ok(Self {
            count: parse(&args[3])?,
            characters: parse(&args[4])?,
            text: match args[5].as_str() {
                "binary" => false,
                "text" => true,
                _ => return Err(io::Error::other("representation must be binary or text")),
            },
        })
    }

    fn key_bytes(&self) -> usize {
        self.characters * if self.text { 8 } else { 4 }
    }
}

fn parse(value: &str) -> io::Result<usize> {
    value.parse().map_err(io::Error::other)
}

fn mark_root(path: &Path) -> io::Result<()> {
    let info = fs::metadata(path)?;
    if !info.is_dir() || info.mode() & 0o777 != 0o700 {
        return Err(io::Error::other("root must be a fresh mode-0700 directory"));
    }
    let mut marker = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path.join(".synthetic-seed-owner.json"))?;
    writeln!(
        marker,
        "{{\"version\":1,\"kind\":\"synthetic-trial\",\"owner_uid\":{},\"device\":{},\"inode\":{},\"creator_pid\":{}}}",
        info.uid(), info.dev(), info.ino(), std::process::id()
    )
}

fn synthetic_key(rank: u64, case: &Case, output: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.clear();
    for position in 0..case.characters {
        // Fixed prefix and eight big-endian rank bytes preserve byte order.
        // Four bytes per character match the width, not the encryption, of OPE.
        let value = if position + 8 < case.characters {
            0x0135_0000 + position as u32 * 65_537
        } else {
            let shift = (case.characters - position - 1) * 8;
            0x1000_0000 + ((rank >> shift) & 255) as u32 * 65_793
        };
        for byte in value.to_be_bytes() {
            if case.text {
                output.push(HEX[usize::from(byte >> 4)]);
                output.push(HEX[usize::from(byte & 15)]);
            } else {
                output.push(byte);
            }
        }
    }
}

fn generate(path: &Path, case: &Case) -> io::Result<()> {
    if case.characters < 8 {
        return Err(io::Error::other("at least eight characters are required"));
    }
    let start = Instant::now();
    let mut file = BufWriter::new(File::create(path)?);
    let mut key = Vec::with_capacity(case.key_bytes());
    for rank in 0..case.count {
        synthetic_key(rank as u64, case, &mut key);
        file.write_all(&key)?;
    }
    file.flush()?;
    println!(
        "{{\"operation\":\"generate\",\"keys\":{},\"characters\":{},\"key_bytes\":{},\"file_bytes\":{},\"elapsed_ms\":{:.6}}}",
        case.count, case.characters, case.key_bytes(), fs::metadata(path)?.len(),
        start.elapsed().as_secs_f64() * 1000.0
    );
    Ok(())
}

struct Timings {
    read: Duration,
    build: Duration,
}

enum LoadedKeys {
    Packed(Vec<u8>),
    Tree(BTreeSet<Vec<u8>>),
}

impl LoadedKeys {
    fn count(&self, key_bytes: usize) -> usize {
        match self {
            Self::Packed(bytes) => bytes.len() / key_bytes,
            Self::Tree(keys) => keys.len(),
        }
    }
}

fn load_packed(file: &mut File, case: &Case) -> io::Result<(Timings, LoadedKeys)> {
    let mut bytes = Vec::with_capacity(case.count * case.key_bytes());
    let start = Instant::now();
    file.read_to_end(&mut bytes)?;
    let read = start.elapsed();
    black_box(&bytes);
    Ok((
        Timings {
            read,
            build: Duration::ZERO,
        },
        LoadedKeys::Packed(bytes),
    ))
}

fn load_tree(file: &mut File, case: &Case) -> io::Result<(Timings, LoadedKeys)> {
    let key_bytes = case.key_bytes();
    let mut chunk = vec![0_u8; key_bytes * 1024];
    let mut keys = BTreeSet::new();
    let mut remaining = case.count;
    let mut read = Duration::ZERO;
    let mut build = Duration::ZERO;
    while remaining != 0 {
        let count = remaining.min(1024);
        let bytes = &mut chunk[..count * key_bytes];
        let start = Instant::now();
        file.read_exact(bytes)?;
        read += start.elapsed();
        let start = Instant::now();
        for key in bytes.chunks_exact(key_bytes) {
            keys.insert(key.to_vec());
        }
        build += start.elapsed();
        remaining -= count;
    }
    black_box(&keys);
    Ok((Timings { read, build }, LoadedKeys::Tree(keys)))
}

fn load(path: &Path, case: &Case, layout: &str) -> io::Result<()> {
    let start = Instant::now();
    let mut file = File::open(path)?;
    let open = start.elapsed();
    let file_bytes = file.metadata()?.len();
    if file_bytes != (case.count * case.key_bytes()) as u64 {
        return Err(io::Error::other("file size differs from requested case"));
    }
    let (timings, keys) = match layout {
        "packed" => load_packed(&mut file, case)?,
        "btree" => load_tree(&mut file, case)?,
        _ => return Err(io::Error::other("layout must be packed or btree")),
    };
    let total = start.elapsed();
    black_box(&keys);
    println!(
        "{{\"operation\":\"load\",\"layout\":\"{}\",\"representation\":\"{}\",\"keys\":{},\"retained_keys\":{},\"characters\":{},\"key_bytes\":{},\"file_bytes\":{},\"open_ms\":{:.6},\"read_ms\":{:.6},\"build_ms\":{:.6},\"total_ms\":{:.6}}}",
        layout, if case.text { "text" } else { "binary" }, case.count,
        keys.count(case.key_bytes()), case.characters, case.key_bytes(), file_bytes,
        open.as_secs_f64() * 1000.0, timings.read.as_secs_f64() * 1000.0,
        timings.build.as_secs_f64() * 1000.0, total.as_secs_f64() * 1000.0
    );
    Ok(())
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 3 && args[1] == "mark-root" {
        return mark_root(Path::new(&args[2]));
    }
    if args.len() < 6 {
        return Err(io::Error::other(
            "use generate|load PATH COUNT CHARACTERS binary|text [packed|btree]",
        ));
    }
    let case = Case::from_args(&args)?;
    let path = Path::new(&args[2]);
    match args[1].as_str() {
        "generate" => generate(path, &case),
        "load" if args.len() == 7 => load(path, &case, &args[6]),
        _ => Err(io::Error::other("unsupported operation")),
    }
}
