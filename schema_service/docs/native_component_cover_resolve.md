# Live resolve: native_component_cover@1 (embedding-beam)

Status: production path (default on)

## What

`POST /v1/schemas/resolve` prefers **`native_component_cover@1`**, the Rust port
of the 2026-07-06 **embedding-beam** component-cover eval (field-embedding
shortlist + single high-coverage match or bounded beam component cover).

Target eval class: ~**99% field cover / ~1% residue** on the locked 64-item
Schema.org cover corpus (see `eval/component_cover.mjs --field-embeddings`).

## Call path

```text
POST /v1/schemas/resolve
  -> SchemaServiceState::resolve_schema_proposals
       -> try_native_component_cover_resolve  (default)
            embed proposal + registry fields
            evaluate_native(config = embedding_beam_shadow_defaults)
            map UseExisting / UseComponents -> SchemaResolveResult
       -> else legacy resolve_schema_proposal_match (descriptive name + renames)
```

## Kill switch

```bash
export SCHEMA_NATIVE_COMPONENT_COVER_RESOLVE=0   # legacy only
```

Unset or any other value → native path enabled.

## Mini / local

Same evaluator: `schema_service_client::local_first_resolver::evaluate_local_proposal`
and Mini `schema_resolver_host` (pack Shadow / EnforceExistingOnly). LiveOnly
Mini modes inherit the service path above after deploy.

Primary Mini live dogfood of pack enforce may wait on cloud-sync Situations;
unit/integration tests of the local path do not require cloud sync.
