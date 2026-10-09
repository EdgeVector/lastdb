# Status gauge contract — North Star proof hooks (PR-5)

North Star: `north-star-lastdb-status-gauge-contract`  
Design: brain `design-lastdb-status-gauge-contract` (PR-5)

## What this PR shipped

1. **Additive `contract` on `/api/status`**  
   `GET /api/status` → envelope `status.contract`:
   ```json
   {
     "version": 1,
     "gauges": [
       {
         "path": "integrity.unresolved_atom_distinct",
         "unit": "edges",
         "unit_noun": "edge(s)",
         "window": "process_lifetime",
         "window_qualifier": "this process",
         "availability": "measured"
       }
     ]
   }
   ```
   Existing status field names and bare-numeric types are **unchanged** (wire freeze).

2. **CLI**  
   `lastdb status --contract` — human lines  
   `lastdb status --contract --json` — same payload as `status.contract`

3. **Rust API (for harnesses)**  
   - `lastdb_node::status_gauge_contract::gauge_contract(&StatusSnapshot)`
   - `lastdb_node::status_gauge_contract::status_value_with_contract(&StatusSnapshot)`
   - `lastdb_node::status_gauge_contract::offline_proof_fixture()` — no daemon
   - `wire_freeze_types_compatible(before, after)`

## How to register / run the North Star proof (after merge)

Terminal card: `lastdb-status-gauge-contract-terminal-proof` (last-stack).

```bash
# Offline (no primary, no live daemon):
last-stack-north-star-proof --offline north-star-lastdb-status-gauge-contract

# Live (ephemeral CoW node only — never primary lastdbd):
NORTH_STAR_PROOF_MODE=live last-stack-north-star-proof north-star-lastdb-status-gauge-contract
```

Done means: `~/.last-stack/north-star-proofs/north-star-lastdb-status-gauge-contract.md`
first line matches `/^PASS/`.

### Suggested harness checks (criteria 1–5)

| # | Check |
|---|--------|
| 1 | Every typed gauge path in `status.contract.gauges` has `unit` + `window`; count of contract gauges ≥ set of gauges used by `lastdb status` renderers |
| 2 | Deserialize a status payload with a converted field **removed** → `Gauge` is `unavailable`, never measured `0` (unit tests already cover this class) |
| 3 | Fault: change a producer's `Unit` → operator noun changes (gate in PR-4 + Display) |
| 4 | Three historical mislabels fail pre-contract renderers / pass typed (PR-2 tests) |
| 5 | Wire freeze: `wire_freeze_types_compatible(pre, post)` on status JSON without relying on `contract` |

Offline fixture for criterion 1 / structure smoke:

```rust
let f = lastdb_node::status_gauge_contract::offline_proof_fixture();
assert!(f.pointer("/status/contract/gauges").unwrap().as_array().unwrap().len() > 0);
```

Or in a shell harness after building this crate's tests:

```bash
cargo test -p lastdb_node --lib status_gauge_contract -- --nocapture
```

## Hard rules

- Never point proof at Tom's primary `~/.lastdb` / live `lastdbd`.
- Use ephemeral CoW + safe-upgrade paths for live mode.
- Do not rename or retype existing `/api/status` fields in this NS.
