# App schema pre-registration proof (dev-first)

Status: living proof for cutover readiness  
Related cards: `schema-preregister-apps-before-cutover`, distribution gate  
Related: [schema registration distribution gate](schema_registration_distribution_gate.md)

## CUTOVER GUARD

Dev-first. This document records **evidence** that known app schemas
resolve on Schema Service. It does **not** flip production hard-require
behavior on Mini declare.

## Replayable registration path

### LastSecrets (catalog resolve + shared-surface publish)

LastSecrets must resolve durable writes to catalog identities. When
sharing/distributing, publish explicit shared-surface metadata via Mini +
Schema Service:

```bash
# Requires Mini with direct catalog declare and shared-surface publish routes,
# plus reachable Schema Service.
cd lastsecrets   # or: bun /path/to/lastsecrets/src/cli.ts
bun run src/cli.ts publish
```

Wire path:

1. Mini direct declare resolves `LastSecret` to an existing catalog identity
   and loads that identity locally (`resolution: "reuse"`).
2. `POST /api/apps/shared-surface/publish-attach` records explicit
   shared-surface metadata when the app is shared.
3. `POST /api/apps/verify-distribution-ready` verifies required identities.

Verified identity (prod Schema Service, 2026-07-14):

| App | Schema | Identity hash | GET /v1/schemas/{hash} |
| --- | --- | --- | --- |
| lastsecrets | LastSecret | `7f2f8d56b5b22ca1ba4a27ced754d80bab3d2defe92ac379e1d85327c5271b82` | 200 present |

Replay verify without re-register:

```bash
# Mini (distribution gate)
curl --unix-socket ~/.lastdb/data/folddb.sock \
  -X POST http://localhost/api/apps/verify-distribution-ready \
  -H 'content-type: application/json' \
  -d '{"app_id":"lastsecrets","schema_identities":["7f2f8d56b5b22ca1ba4a27ced754d80bab3d2defe92ac379e1d85327c5271b82"]}'

# Direct Schema Service (prod URL from folddb_profile/environments.json)
curl -sS -o /dev/null -w '%{http_code}\n' \
  "$SCHEMA_SERVICE_URL/v1/schemas/7f2f8d56b5b22ca1ba4a27ced754d80bab3d2defe92ac379e1d85327c5271b82"
```

Or use the checked-in script:

```bash
bash schema_service/scripts/verify_app_schema_preregister.sh
# optional: SCHEMA_SERVICE_URL=https://... bash schema_service/scripts/verify_app_schema_preregister.sh
```

## fbrain / fkanban still resolve

Probed against **prod** Schema Service
(`environments.json` → `prod.schema_service`) on 2026-07-14 after LastSecrets
publish. All listed hashes returned **HTTP 200**.

### fbrain (sample of configured kinds)

Hashes from `~/.fbrain/config.json` `schemaHashes`:

| Kind | Identity hash |
| --- | --- |
| concept | `8838f066b34a72fd0ad3d4c22e36a09c1c1150d81d1c6bf5bff4176276ed4fe7` |
| task | `4a67db42689be7c1c85b51df6d2f9dab8c292596805b6885286e9652cd3b2d0e` |
| design | `1aac3ad7b6d111689ec336adc7efe5efa0cd3b8b4aae2da05808520897b4183e` |
| preference | `6440d047d5fb73d3b2e1c1018411730c4eeaa724aea9bca390bdfdd2878d5bfe` |
| reference | `5c691083b7dfb389a583e7d5f888a2735fe5608f43ff8970f25b503d65e8d4a4` |
| agent | `c9c5dd08d8adf83f5a30920bfc6889219d28ef2e6debff01cc3408b3bcd04632` |
| project | `de0a66b55bbe9b027d3189c95a0d1cd46fd4624bdc313f266666bda159690502` |
| spike | `a29b9e5b772a66741b971881ccebcd8826965cfb09a285ecd54a6e9d78eb3762` |
| sop | `57b2b2d9aaf546b0e418c8ce2033f253a97fe77ed0f6e893733a6e777c390063` |
| decision | `61b22ab49a359c1af27692757a080c4708018e36190c9a51df4806392f925d7c` |

### fkanban

Hashes from `~/.fkanban/config.json` `schemaHashes`:

| Kind | Identity hash |
| --- | --- |
| card | `eacad7322a1eb2daa26e389426c160e522c682d4cbcdf601c6df7093421122db` |
| board | `53bef1f61388fe0c219f5e6310f68f2bcfeef5d9513477b09bb8b3d6b4584275` |

## Endpoint class

| Item | Value |
| --- | --- |
| Environment | **prod** Schema Service (not a local actix binary) |
| URL source | `folddb_profile/environments.json` → `environments.prod.schema_service` |
| Mini | brew lastdb with direct catalog declare + shared-surface publish routes |
| Production hard-require on declare | **Not flipped** (follow-on: `schema-mini-declare-service-required`) |

## What this unblocks

The Mini declare hard-require follow-on has:

1. A replayable LastSecrets registration path (`lastsecrets publish`).  
2. A known LastSecret identity hash present on the service.  
3. Confirmation that fbrain/fkanban identities still resolve after that
   registration.

## Non-goals

- Promoting fbrain/fkanban private schemas into the shared-only resolver
  projection (they remain private / legacy until explicit shared-surface
  publish/attach).  
- Offline registration queue (separate backlog card).  
- WASM / transform / view / desktop DMG paths.
