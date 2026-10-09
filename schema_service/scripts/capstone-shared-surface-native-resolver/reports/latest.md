# Shared-Surface Native Resolver Capstone Report

Generated: 20260717T165712Z
STATUS: PASS

This report is sanitized. It records commands, status, public endpoints, and
non-secret artifact counts only. API keys are resolved by LastSecrets at point
of use and are never printed here.

## Inputs

- Repo: /Users/example/.fkanban/worktrees/schema-shared-surface-native-resolver-capstone
- Commit: b953d0c80d945262aa32b5a6bfa9e7299baca5ac
- LASTDB_HOME: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/lastdb-home
- Snapshot artifact: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/shared-only-snapshot.json
- Log directory: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/logs
- Public pack origin: https://pub-a27a88cf1c0b47cdb158819600250b98.r2.dev
- Public pack channel: dev
- Public pack pointer: schema-resolver-packs/dev/latest/contract-v1/native_component_cover-v1/embedder-sha256-8f59a052f3e17a28982df924bc4eb42889054e2c82d63089f793eb78dc5c1263/manifest.json

## Proof Steps

- Prod shared-only snapshot fetch (LastSecrets locator only): RUNNING
- Prod shared-only snapshot fetch (LastSecrets locator only): PASS (log: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/logs/20260717T165712Z-prod-shared-only-fetch.log)
- Prod shared-only snapshot structure and counts: RUNNING
- Prod shared-only snapshot structure and counts: PASS (log: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/logs/20260717T165712Z-prod-shared-only-validate.log)
- Prod shared-only snapshot summary: format_version=2 version=2569 schemas=939
- Anonymous public resolver-pack latest pointer: RUNNING
- Anonymous public resolver-pack latest pointer: PASS (log: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/logs/20260717T165712Z-public-pack-pointer.log)
- Resolver pack consumer fixtures: RUNNING
- Resolver pack consumer fixtures: PASS (log: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/logs/20260717T165712Z-resolver-pack-consumer.log)
- Resolver pack manifest fixtures: RUNNING
- Resolver pack manifest fixtures: PASS (log: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/logs/20260717T165712Z-resolver-pack-manifest.log)
- Public resolver pack bootstrap load: RUNNING
- Public resolver pack bootstrap load: PASS (log: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/logs/20260717T165712Z-pack-dogfood-http-load.log)
- Mini resolver host route and mode guards: RUNNING
- Mini resolver host route and mode guards: PASS (log: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/logs/20260717T165712Z-lastdb-node-schema-resolver-host.log)
- Mini semantic-search dependency closure has no Wasmtime/Cranelift: RUNNING
- Mini semantic-search dependency closure has no Wasmtime/Cranelift: PASS (log: /var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T//schema-capstone-20260717T165712Z.SsqvXW/logs/20260717T165712Z-no-wasmtime-cranelift.log)

The capstone runner proved the production shared-only snapshot, the public
resolver-pack origin, resolver-pack bootstrap loading, Mini shared-surface
route ownership, direct-declare resolver ownership, and the no-Wasmtime
Mini semantic-search dependency invariant.

## Result

STATUS: PASS
