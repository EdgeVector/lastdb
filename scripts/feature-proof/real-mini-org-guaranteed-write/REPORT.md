# Real Mini organization guaranteed-write proof

Date: 2026-09-22

## Purpose

Prove one multi-slot guaranteed write with two separate ephemeral Mini nodes.
The nodes use two DEV Exemem identities and one shared organization database.

## Setup

The runner creates two fresh homes under `/private/tmp`. It does not use the
primary LastDB home.

1. It reads the DEV bootstrap key from LastSecrets.
2. It mints and consumes one DEV invite for node A.
3. Node A mints and consumes one DEV invite for node B.
4. It starts one `lastdbd` Mini per home.
5. Node A creates an organization target and claims its cloud head.
6. Node A grants node B the `writer` role.
7. Node B registers the same organization target.

The runner removes its homes after a normal run. It does not print API keys,
recovery phrases, or E2E keys.

## Test data

Each contender sends one `guaranteed_write_set_cas` request. The request has
three absent slots:

| Field | Slot value |
| --- | --- |
| `git_ref_oid` | `main` |
| `merge_fence` | `cr-42` |
| `pack_digest` | `repo-main` |

Node A uses version `node-a`. Node B uses version `node-b`.

## Run

Run this command from the Fold repository root:

```sh
python3 scripts/feature-proof/real-mini-org-guaranteed-write/runner.py
```

The runner starts both CAS calls through a two-worker executor. It then stops
and restarts node B. Node B reads the shared head after its restart.

## Result

The DEV run passed on 2026-09-22.

```json
{
  "ok": true,
  "org_hash": "7e2c14504a05672de1777f5b43fafdcd8630e34bb4eb9f8a86a96355007987bb",
  "slot_count": 3,
  "winner": "node-a",
  "loser": "node-b",
  "peer_restart": "node-b",
  "peer_slot_versions": ["node-a"],
  "grant_acknowledged_complete_set": true
}
```

One call won. The other call did not change a subset of the slots. After its
restart, node B read all three slots at version `node-a`. The proof also
checks that the recorded grant names exactly those three slots.

## Service requirement found by this proof

Guaranteed-write heads use a conditional PUT. B2 rejects that operation.
The head now routes to R2 under `{scope}/ver/head`. R2 supports conditional
PUT operations.

The runner calls the storage guaranteed-write protocol with each Mini's own
credential. Mini does not yet expose a generic UDS route for arbitrary
guaranteed schema-field mutations.
