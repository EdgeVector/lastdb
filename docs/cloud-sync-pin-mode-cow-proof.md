# Cloud Sync Pin-Mode CoW Proof Harness

`scripts/cloud-sync-pin-mode-cow-proof.sh` prepares a throwaway LastDB home from
a primary-home CoW clone or copy, refuses to run against primary `~/.lastdb` or
legacy `~/.folddb`, records the F0 pin boundary, and writes proof artifacts
under `~/.local/state/last-stack/cloud-sync-pin-mode-cow-proof` by default.

Dry/probe mode is the default:

```bash
./scripts/cloud-sync-pin-mode-cow-proof.sh
```

To exercise deterministic sealed-chunk writes against the cloned home only,
build the writer and enable writes:

```bash
cargo build -p lastdb_node --bin csync_e2e_write
EXECUTE_WRITES=1 \
CSYNC_E2E_WRITE=./target/debug/csync_e2e_write \
./scripts/cloud-sync-pin-mode-cow-proof.sh
```

The harness disables inherited `cloud_sync.json` on the clone unless
`ALLOW_COW_CLOUD_SYNC=1` is explicitly set. That keeps this harness from
accidentally connecting a proof home to the live cloud-sync account before the
terminal restore proof is ready.
