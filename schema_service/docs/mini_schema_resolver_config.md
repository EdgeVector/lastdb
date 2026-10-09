# Mini schema resolver install config

Status: implemented (shared-surface pack runtime wiring)

Related: [local_schema_resolver_config.md](local_schema_resolver_config.md),
[direct_schema_registration.md](direct_schema_registration.md), shared-surface
publish/attach facade.

## File

`{LASTDB_HOME}/schema_resolver.json` (default home `~/.lastdb`).

Missing file ⇒ **LiveOnly** (safe default). LiveOnly still means catalog-backed
resolution; it does not enable free local durable mint.

## Example

```json
{
  "enabled": true,
  "mode": "shadow",
  "allow_enforce": false,
  "kill_switch": false,
  "base_url": "https://resolver.example.invalid/schema-resolver-packs",
  "channel": "prod",
  "refresh_interval_seconds": 3600,
  "refresh_jitter_seconds": 60,
  "request_timeout_seconds": 10,
  "max_download_bytes": 134217728,
  "max_config_age_seconds": 604800,
  "enforce_gate": {
    "unsafe_reuse_count": 0,
    "adversarial_note_event_trip_passed": true,
    "match_precision": 0.99,
    "match_precision_floor": 0.99,
    "field_coverage": 0.95,
    "field_coverage_floor": 0.95,
    "required_field_coverage": 1.0,
    "required_field_coverage_floor": 1.0,
    "p95_latency_ms": 42,
    "p95_latency_budget_ms": 100,
    "live_fallback_on_ambiguity_or_miss": true,
    "bad_latest_lkg_drill_passed": true,
    "kill_switch_forces_live_only": true
  },
  "expected_embedder_id": "fastembed/all-MiniLM-L6-v2",
  "cache_dir": "schema-resolver",
  "trusted_keys": [
    {
      "key_id": "resolver-release-2026-01",
      "public_key_b64": "<ed25519-verifying-key-base64>"
    }
  ]
}
```

## Dev Public Pack Origin

The dev resolver-pack channel is published to the public R2 bucket origin:

```text
base_url: https://pub-a27a88cf1c0b47cdb158819600250b98.r2.dev
channel: dev
latest manifest: schema-resolver-packs/dev/latest/contract-v1/native_component_cover-v1/embedder-sha256-8f59a052f3e17a28982df924bc4eb42889054e2c82d63089f793eb78dc5c1263/manifest.json
```

Trusted dev signing key:

```json
{
  "key_id": "e5ecd9135cac5b41ec799694b5a6f8370d5afa4a5a9642d336fb09898810f490",
  "public_key_b64": "4o7rMoWiSbo4MGykaa8Yq2VEFUHMoBZonX+HAGCr2yU="
}
```

Minimal dev config:

```json
{
  "enabled": true,
  "mode": "shadow",
  "allow_enforce": false,
  "kill_switch": false,
  "base_url": "https://pub-a27a88cf1c0b47cdb158819600250b98.r2.dev",
  "channel": "dev",
  "refresh_interval_seconds": 3600,
  "refresh_jitter_seconds": 60,
  "request_timeout_seconds": 10,
  "max_download_bytes": 134217728,
  "max_config_age_seconds": 604800,
  "enforce_gate": {
    "unsafe_reuse_count": 1,
    "adversarial_note_event_trip_passed": false,
    "match_precision": 0.0,
    "match_precision_floor": 0.99,
    "field_coverage": 0.0,
    "field_coverage_floor": 0.95,
    "required_field_coverage": 0.0,
    "required_field_coverage_floor": 1.0,
    "p95_latency_ms": null,
    "p95_latency_budget_ms": null,
    "live_fallback_on_ambiguity_or_miss": false,
    "bad_latest_lkg_drill_passed": false,
    "kill_switch_forces_live_only": false
  },
  "expected_embedder_id": "fastembed/all-MiniLM-L6-v2",
  "cache_dir": "schema-resolver",
  "trusted_keys": [
    {
      "key_id": "e5ecd9135cac5b41ec799694b5a6f8370d5afa4a5a9642d336fb09898810f490",
      "public_key_b64": "4o7rMoWiSbo4MGykaa8Yq2VEFUHMoBZonX+HAGCr2yU="
    }
  ]
}
```

Published proof, 2026-07-15 UTC:

```bash
BASE_URL=https://pub-a27a88cf1c0b47cdb158819600250b98.r2.dev
POINTER=schema-resolver-packs/dev/latest/contract-v1/native_component_cover-v1/embedder-sha256-8f59a052f3e17a28982df924bc4eb42889054e2c82d63089f793eb78dc5c1263/manifest.json
curl -fsS -o /dev/null -w "%{http_code}\n" "$BASE_URL/$POINTER" # 200

DOGFOOD_PACK_BASE_URL="$BASE_URL" \
DOGFOOD_PACK_KEY_ID=e5ecd9135cac5b41ec799694b5a6f8370d5afa4a5a9642d336fb09898810f490 \
DOGFOOD_PACK_PUBLIC_KEY_B64=4o7rMoWiSbo4MGykaa8Yq2VEFUHMoBZonX+HAGCr2yU= \
  cargo test -p schema_service_client --test pack_dogfood_http_load -- --nocapture
```

## Modes

| `mode` | Behavior |
| --- | --- |
| `live_only` | Kill-switch-friendly default: always live Schema Service |
| `shadow` | Live answers unchanged; local pack compare for telemetry |
| `enforce_existing_only` | Local catalog reuse when confident; requires `allow_enforce: true` and a passing `enforce_gate` report |

`kill_switch: true` or `enabled: false` ⇒ LiveOnly.

Enforce without `allow_enforce`, or with a failing/missing `enforce_gate`, clamps
to **shadow**. The gate is machine-decided by
`schema_resolver_enforce_gate_pass()` and requires:

- zero unsafe-reuse disagreements, including adversarial note/event/trip fixtures
- match precision and field coverage at or above the configured floors
- ambiguity/miss fixtures proving live fallback
- bad-latest proving last-known-good fallback
- kill switch evidence proving LiveOnly
- p95 proposal embed + resolve latency under budget when a latency budget is set

No mode permits Mini to create catalog identities locally or store app-local
field dialects as the durable representation.

## Env overlays

| Env | Effect |
| --- | --- |
| `SCHEMA_RESOLVER_ENABLED` | bool |
| `SCHEMA_RESOLVER_MODE` | live_only / shadow / enforce_existing_only |
| `SCHEMA_RESOLVER_ALLOW_ENFORCE` | bool |
| `SCHEMA_RESOLVER_KILL_SWITCH` | bool → LiveOnly |
| `SCHEMA_RESOLVER_BASE_URL` | HTTPS pack origin |
| `SCHEMA_RESOLVER_CHANNEL` | dev / prod |
| `SCHEMA_RESOLVER_EXPECTED_EMBEDDER_ID` | retained for config compatibility |
| `SCHEMA_RESOLVER_TRUSTED_KEYS` | `key_id:b64,key_id:b64` |

## Binary requirements

Shadow/Enforce pack evaluation is no longer performed inside Mini because the
in-process FastEmbed proposal embedder was removed from `fold_db`/`lastdb_node`.
Mini source and release builds clamp pack-backed modes to LiveOnly even if this
config requests Shadow/Enforce. Schema-service operator tooling still owns
FastEmbed pack generation.

## Routes

- `POST /api/apps/shared-surface/publish-attach` — uses this wiring
- `GET /api/apps/shared-surface/attachments` — local attachment list

Schema Service live `POST /v1/schemas/resolve` uses the same
**`native_component_cover@1`** (embedding-beam) algorithm as the signed pack
artifacts. Mini parses pack-mode config for compatibility, but currently clamps
`shadow` / `enforce_existing_only` to LiveOnly because there is no local
proposal embedder in the Mini binary.

`POST /api/apps/declare-schema` is the canonical app catalog-sync surface.
`POST /api/schemas/declare` is a strict compatibility adapter into the same
implementation, not a local-mint or migration surface. Both resolve/register
first, exact-load catalog mapper metadata, append a durable schema-sync audit
event, and only then return a bind-eligible identity. Schema key migration is a
separate operation and is never inferred by either route.

## Schema Service live resolve gate

On the service, `SCHEMA_NATIVE_COMPONENT_COVER_RESOLVE` defaults **on**. Set to
`0`/`false`/`off` to force legacy descriptive-name resolve only (kill switch).

## Capstone

The shared-surface native-resolver capstone is runnable from a throwaway Mini
home and writes a sanitized report:

```bash
LASTDB_HOME=/tmp/schema-capstone-$$ \
  bash schema_service/scripts/capstone-shared-surface-native-resolver/run.sh
```

The runner fetches the production shared-only snapshot through
`lastsecrets://schema-service-prod-api-key`, probes the public resolver-pack
origin anonymously, runs the resolver-pack and Mini host-facade guard tests, and
checks that schema-service FastEmbed tooling remains isolated from the Mini
product dependency closure. The output report is
`schema_service/scripts/capstone-shared-surface-native-resolver/reports/latest.md`.
