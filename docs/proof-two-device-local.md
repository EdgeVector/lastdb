# Two-device proof on one machine (no cloud)

This page shows how to run two LastDB homes of one owner on one machine, and
how to move their mutation-log segments between them through a directory. You
can then make a real two-device conflict, and read what each node did with it.

The tooling is for developers. It is guarded so it cannot run on a real home.

## What it does and does not prove

It runs the real code for these steps:

- capture of each write into the durable mutation log;
- the order, the seal (encryption) and the frontier of each segment;
- the peer-apply path on the other home, including the `MutationIntent` replay
  and the tip decision.

It replaces only the HTTP transport. A cloud is not involved, so it does not
prove anything about cloud auth, upload, download, listing, backup or restore.

## The two verbs

Both are hidden from `lastdb db --help` and exist only on the owner socket.

```text
lastdb db export-mutation-log --to <dir>      # seal pending records, write segments
lastdb db replay-mutation-log --from <dir>    # apply the segments of other writers
```

`export` runs the same scan, seal and confirm steps as the cloud upload cycle.
After it, the exported records count as published, as they do after a cloud
upload. A second `export` writes only newer records.

`replay` hands the segments to the same function that the live peer-apply cycle
uses. It skips the segments of its own device. It persists the applied frontier,
so a second `replay` of one directory applies nothing.

Both homes may use one directory.

## The guard

The daemon refuses unless every condition holds:

1. The daemon environment has `LASTDB_PROOF_REPLAY=1`.
2. The home holds a regular file named `.synthetic-home`. You create it.
3. The home is not `~/.lastdb` or `~/.folddb`, a symlink target of either, a
   path inside one, or a path above one. The check also uses the home that the
   operating system account database names, so a changed `$HOME` does not hide
   the real home.
4. The home has no cloud configuration (`cloud_sync.json` or its paused form).
5. The daemon booted with the local proof engine. That engine is built only
   when conditions 1 to 4 hold at boot. It is a sync engine that captures
   writes and replays segments, with an unresolvable `.invalid` address and an
   HTTPS-only client, so no call can leave the machine.

If the variable is set but the guard fails at boot, the daemon boots as a
normal local-only node and logs the refusal. It never fails boot.

Each verb checks conditions 1 to 4 again on every call. Every use logs a WARN
line. The boot logs `PROOF TOOLING ARMED`.

The daemon with the proof engine shuts down in the local-only mode (no final
cloud sync), because it has no cloud.

## Make two homes of one owner

Two devices of one owner share one identity seed (`identity.key`, 32 bytes) and
have two different device ids (`data/.device_id`).

Use a synthetic seed only. Never copy the seed of a real home.

```bash
HA="$(mktemp -d "$TMPDIR/proofA.XXXXXX")"
HB="$(mktemp -d "$TMPDIR/proofB.XXXXXX")"
XD="$(mktemp -d "$TMPDIR/proofX.XXXXXX")"     # the exchange directory
touch "$HA/.synthetic-home" "$HB/.synthetic-home"
```

Keep the home path short: a Unix socket path is limited to about 100 bytes.

Start home A. It creates a fresh identity and its own device id.

```bash
env LASTDB_HOME="$HA" FOLDDB_HOME="$HA" LASTDB_PROOF_REPLAY=1 EXEMEM_ENV=dev \
    LASTDB_SUPPRESS_RECOVERY_PHRASE=1 RUST_MIN_STACK=67108864 \
    lastdbd --data-dir "$HA" &
```

Copy the seed of A to B before B starts for the first time. B then has the
same owner. B creates its own device id on its first start.

```bash
cp "$HA/identity.key" "$HB/identity.key" && chmod 600 "$HB/identity.key"
env LASTDB_HOME="$HB" FOLDDB_HOME="$HB" LASTDB_PROOF_REPLAY=1 EXEMEM_ENV=dev \
    LASTDB_SUPPRESS_RECOVERY_PHRASE=1 RUST_MIN_STACK=67108864 \
    lastdbd --data-dir "$HB" &
```

Check that the two homes are two devices of one owner:

```bash
lastdb --data-dir "$HA" status | grep -E 'Home|Owner'    # same Owner on both
cat "$HA/data/.device_id" "$HB/data/.device_id"           # two different ids
```

Notes:

- A debug build overflows the 8 MiB worker stack on a write. Use a release or
  `dev-release` build, or set `RUST_MIN_STACK=67108864`.
- `EXEMEM_ENV=dev` makes a schema declaration use the DEV Schema Service. A node
  without it uses the production service. Do not declare proof schemas there.
- Set `FOLD_SYNC_DEVICE_ID` to choose a device id instead of a random one.

## Make a conflict and read it

Declare the same schema on both homes (`POST /api/schemas/declare` on the owner
socket). Use a `Hash` schema when you want to read the conflict state, because
`POST /api/conflicts/keys` supports only `Hash` schemas.

Write the same key on both homes at nearly the same time, with two values.
Then exchange in both directions:

```bash
lastdb --data-dir "$HA" db export-mutation-log --to "$XD"
lastdb --data-dir "$HB" db export-mutation-log --to "$XD"
lastdb --data-dir "$HB" db replay-mutation-log --from "$XD"
lastdb --data-dir "$HA" db replay-mutation-log --from "$XD"
```

Reads that work on a synthetic home:

| Read | What it shows |
|---|---|
| `POST /api/query` with a key | The tip on each node. |
| `POST /api/conflicts/keys` | `clean` or `conflict` for a `Hash` field molecule. |
| `GET /api/history/<molecule>` | The recorded mutation events of one molecule. |
| `GET /api/atom/<atom_uuid>` | Whether an atom body is still on disk. |
| `lastdb db inventory --json` | Atoms per schema, and the `history:` and `conflict:` key classes. |
| `GET /api/status`, field `status.gc_atoms` | `last_lap_unreferenced_atoms`, `peer_apply_state`, `total_atoms_deleted`. |

To age garbage in seconds on a synthetic home, start the daemon with
`LASTDB_GC_ATOMS_START_DELAY_SECS=1 LASTDB_GC_ATOMS_STEP_INTERVAL_SECS=2
LASTDB_GC_ATOMS_RELAP_INTERVAL_SECS=5 LASTDB_GC_ATOMS_GRACE_WINDOW_SECS=3
LASTDB_GC_ATOMS_ALLOW_SHORT_GRACE=1` and `LASTDB_SELF_METRICS_SAMPLE_SECS=1`.
After a peer apply, the interlock keeps the delete pass off; the proof can lift
it with `LASTDB_GC_ATOMS_ALLOW_AFTER_PEER_APPLY=1`. Never set these on a home
that holds data you need.

## Stop and clean up

Send `SIGTERM` to each daemon you started. Remove only the temporary homes you
made. Keep the receipts you need first.
