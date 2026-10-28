# Mini schema-service configuration

Mini resolves schema proposals and shared-surface publish/attach requests through
the live schema service. It has no local proposal embedder or resolver-pack route.
The server still owns its matching engine and canonical schema registry.

`<home>/schema_resolver.json` optionally supplies one active setting:

```json
{ "request_timeout_seconds": 10 }
```

The owner-socket request budget clamps this value to 1–20 seconds. Both the HTTP
client and the outer operation deadline use that budget. Missing, blank or invalid
files use the ten-second default. Other JSON properties are ignored, including
retired pack settings. A malformed retired property's type no longer invalidates
an otherwise valid timeout setting.

The former enabled/mode/allow-enforce/kill-switch, pack origin/channel, refresh,
download, age, gate-report, embedder, cache and trusted-key settings are retired.
The `SCHEMA_RESOLVER_*` environment overlays have no effect. These settings
previously always fell back to live-only in Mini; their diagnostic warnings are
removed along with the unavailable modes.

Schema-service endpoint selection, publish node signatures, shared-surface
validation, proposal-ID matching and live registration rules remain in effect.
