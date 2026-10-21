# Run proofs for incremental compaction, steps S0, S1 and S2

This directory holds the scripts and the numbers for three steps of the plan to bound the parts and the
write gate (the plan itself is not in this repository; its steps are named here as they are used):

- S0: the measurement scripts, kept as run evidence.
- S1, cheap pack: the full pack keeps the facts it has proved (block-map pages, part headers, a few decoded
  blocks, open part files) for the whole pack and finishes the record digest once. No byte on disk changes.
- S2, one locator object: a new group takes a fresh part in an existing locator object. A batch makes 4 parts
  on a home that was never packed.

There are no tests here. Every proof is a run with the built binaries on synthetic homes.

## Rules for every script

- A home is a fresh `mktemp`-style directory under `$TMPDIR`, named `s1s2.*`. `ldc.refuse_primary` refuses
  any path inside a default home of the product (a dot directory directly under the user home that starts
  with `.lastdb`, `.folddb` or `.primary`). The scripts never copy, open or list such a home.
- Every node runs with `EXEMEM_ENV=dev`, and `Node.start` checks that `lastdb status` names the temporary
  home before any write.
- A node that a node proof starts is a child of the script. A kill is a SIGKILL of that child.
- `ldc.remove_owned` deletes only an `s1s2.*` entry directly under `$TMPDIR`.
- Clones are `cp -c` (copy-on-write) clones of my own synthetic homes. A snapshot is never changed.
  A node needs a short path (a Unix socket path is limited to about 100 bytes), so node clones use
  `ldc.clone_path`.

## Binaries

`LDBIN` names a directory with `lastdb`, `lastdbd`, `lastdb-pack-maintain`, `lastdb-disk-home`,
`lastdb-journal` and `lastdb-disk-recovery`. Two directories are used: `main` (a build of the commit that this
change branches from) and `new` (a build of this change). Both builds use the `dev-release` profile:

```
cargo build --profile dev-release --locked -p lastdb_node --bin lastdb --bin lastdbd \
  -p laststore --bin lastdb-pack-maintain --bin lastdb-disk-home --bin lastdb-journal --bin lastdb-disk-recovery
```

The oracle always reads with the `main` binaries, so the reader does not share the code under test.

## Scripts

| Script | What it does |
|---|---|
| `ldc.py` | Library: start and kill a node, write and read through its HTTP routes, decode the receipt of a stopped home. |
| `ingest.py` | Build a never-packed home with real writes (500 operations a request, 8 clients, 256 groups). Snapshots at stages. |
| `oracle.py` | Record oracle of a stopped home: count, count for each collection, order-free hashes of (collection, key, body SHA-256, batch, operation). |
| `oracle_pack.py` | Oracle before and after a pack, for any number of pack binaries and readers. `--twice` packs the packed home again. |
| `crafted_home.py`, `crafted_verify.py` | A crafted home (overwrites, tombstones, edge sizes, 1 MiB bodies across a part boundary, long keys) and a readback of every key. |
| `readback.py` | Pack a clone, start a node on it, read every acknowledged key. |
| `pack_speed.py` | Time `compact --grace-ms 0` of a binary on a clone: wall, CPU, peak memory. |
| `profile_pack.py` | `/usr/bin/sample` profile of the pack. |
| `parts_per_request.py` | Parts a request adds on a never-packed home (S2). |
| `kill_chain.py` | SIGKILL chain with the oracle on each killed home and a readback of every key. |
| `backup_restore.py` | Backup and restore of a home with one locator object. |
| `listing_cost.py` | Cost of many part files in one directory. |
| `node_pack.py` | Gate hold of the automatic pack in a node with writers. |

## Commands

`$MAIN` and `$NEW` are the two binary directories. `$HOME_DIR` names a synthetic home that a script made.
Run the scripts from this directory. They need only Python 3.9 and the standard library.

```
# S0: the homes. Never packed, auto maintenance off, 500 operations a request, 8 clients, 256 groups.
LDBIN=$MAIN python3 ingest.py --stages 14000,140000,200000 --tag big
#   14,000 operations = 98,553 records; 140,000 = 980,553 records; 200,000 = 1,400,553 records.
#   The script prints the path of each stage snapshot.

# S1, proof 1: the record oracle at about 100 thousand and about 1 million records.
python3 oracle_pack.py --home $SNAP14K  --oracle main=$MAIN --pack main=$MAIN,new=$NEW --reader new=$NEW --twice
python3 oracle_pack.py --home $SNAP140K --oracle main=$MAIN --pack main=$MAIN,new=$NEW --reader new=$NEW
#   The oracle reads the home before, and each packed clone after, with the binaries of main and of this change.

# S1, proof 1: the crafted homes (one made by the binaries of main, one by the binaries of this change).
LDBIN=$MAIN python3 crafted_home.py ; LDBIN=$NEW python3 crafted_home.py      # each prints a home path
python3 oracle_pack.py --home $CRAFTED --expected $CRAFTED.expected.json --oracle main=$MAIN \
  --pack main=$MAIN,new=$NEW --reader new=$NEW --twice

# S1, proof 1: every acknowledged key reads back from a node on the packed clone.
LDBIN=$NEW python3 readback.py --home $SNAP14K  --ops 14000  --pack-bin $NEW
LDBIN=$NEW python3 readback.py --home $SNAP140K --ops 140000 --pack-bin $NEW

# S1, proof 2 and 4: speed, CPU and peak memory, on a clone of the 1.4 million record home.
python3 pack_speed.py --home $SNAP200K --bins main=$MAIN,new=$NEW --runs 2
python3 profile_pack.py --home $SNAP200K --bin $NEW  --out profile-new.txt  --after 15 --seconds 20
python3 profile_pack.py --home $SNAP200K --bin $MAIN --out profile-main.txt --after 15 --seconds 20

# S1, proof 5: the gate hold of the automatic pack in a node, with 4 writers.
LDBIN=$MAIN python3 node_pack.py --home $SNAP200K
LDBIN=$NEW  python3 node_pack.py --home $SNAP200K

# S2, proof 3: parts for each request, before and after.
LDBIN=$MAIN python3 parts_per_request.py --label main
LDBIN=$NEW  python3 parts_per_request.py --label new
# S2: the same home shape at scale (the part count of the 1.4 million record home).
LDBIN=$NEW python3 ingest.py --stages 14000,200000 --tag s2
# S2: 20-round SIGKILL chains, without maintenance and with maintenance on all the time.
LDBIN=$NEW python3 kill_chain.py --rounds 20 --maintenance off      --oracle-bin $MAIN
LDBIN=$NEW python3 kill_chain.py --rounds 20 --maintenance frequent --oracle-bin $MAIN
# S2: backup and restore of a home with one locator object.
LDBIN=$NEW python3 backup_restore.py --oracle-bin $MAIN
# S2: cost of many part files in one directory.
python3 listing_cost.py
```
