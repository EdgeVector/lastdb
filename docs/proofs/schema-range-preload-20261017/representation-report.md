# Synthetic range-key memory measurements

The runs measure two memory forms for ordered range keys.
They do not measure a complete LastDB schema preload.
They do not change the 10,000-record warm set limit.

At 100,000 binary keys of length R40, the packed form loads in 1.34 ms.
The mutable tree loads in 19.90 ms.
The peak process sizes are 16.91 MiB and 22.47 MiB.

## Results

Each time is the median of fresh processes.
Each memory size is the median peak size of the complete process.
One MiB is 1,048,576 bytes.
The source stops the load timer before it frees the data.

| Keys | Range characters | Form | File MiB | Packed load ms | Tree load ms | Packed peak MiB | Tree peak MiB |
|---:|---:|---|---:|---:|---:|---:|---:|
| 10,000 | 24 | Binary | 0.916 | 0.104 | 1.304 | 2.547 | 3.203 |
| 10,000 | 40 | Binary | 1.526 | 0.148 | 1.745 | 3.156 | 3.859 |
| 10,000 | 54 | Binary | 2.060 | 0.198 | 2.303 | 3.703 | 4.547 |
| 100,000 | 24 | Binary | 9.155 | 0.825 | 15.016 | 10.797 | 16.266 |
| 100,000 | 40 | Binary | 15.259 | 1.335 | 19.899 | 16.906 | 22.469 |
| 100,000 | 54 | Binary | 20.599 | 1.783 | 26.665 | 22.250 | 28.656 |
| 100,000 | 54 | Text | 41.199 | 3.599 | 43.222 | 42.844 | 50.969 |
| 1,000,000 | 40 | Binary | 152.588 | 12.786 | 243.006 | 154.234 | 208.516 |

The runs use five samples per form through 100,000 keys.
The one-million-key case uses three samples per form.
The largest peak process size is 218,693,632 bytes, or 208.56 MiB.
The process stays below the requested 500 MB limit.

## Inputs and methods

The program creates fixed-width files with deterministic ordered unique keys.
R is the number of synthetic range characters.
The binary form uses four bytes per character.
The text form uses eight lowercase hex characters per character.
The binary form stores the same synthetic words without the hex expansion.

Each key has a fixed prefix and an ordered counter in its last eight words.
The counter supplies one byte to each word.
The program creates the files before any load timer starts.
The load does not sort the keys.
The program performs no encryption.

The packed form reads one file into a contiguous `Vec<u8>`.
Fixed-width keys need no separate offset list in this experiment.
The mutable form inserts each key into `BTreeSet<Vec<u8>>`.
It uses one input buffer for 1,024 keys.
It does not retain a second complete input copy.

The raw receipts separate file open, file read, and tree construction time.
For 100,000 binary R40 keys, median file read takes 0.753 ms in the tree form.
Tree construction takes 19.111 ms.
For one million binary R40 keys, those times are 8.224 ms and 233.935 ms.
The tree uses ordinary ordered inserts, without a special bulk constructor.

## Limits

The generated files remain in the OS cache.
No cache flush occurs.
The host has no exclusive reservation for these runs.
All 76 process receipts report zero block input operations and zero page faults.
These results do not measure a first read from the physical device.
The process size includes the program, allocator, input buffer, keys, and tree nodes.
No empty-process size is subtracted.

The keys contain only synthetic range words.
They exclude schema and molecule names, hash bytes, separators, tips, atoms, metadata, and delete state.
They exclude the disk tree traversal and the LastDB encryption and decode work.
They exclude core path admission, schema controls, and accepted writes.
A real schema preload must include those costs.

Real keys can have different lengths and prefixes.
A variable-length packed form needs boundaries or offsets.
A complete preload must also define its update, delete, memory cap, and publication rules.
These results establish only the cost of the two measured memory forms.

## Receipts

- `representation.rs`: standalone Rust source.
- `representation-manifest.json`: compiler, host, source hash, binary hash, and method.
- `representation-run/summary.json`: all samples and per-case median, minimum, and maximum.
- `representation-run/*.json`: raw generation and load phase receipts.
- `representation-run/*.time`: raw `/usr/bin/time -l` process receipts.
- `representation-run/input-sha256.txt`: hashes of the generated input files.
- `summarize_representation.py`: receipt summary source.

The files and the executable use one fresh owned temporary root.
The supported exact-path cleanup removes that root after receipt preservation.
The cleanup command returns exit code zero.
No LastDB node or database home is part of this measurement.
