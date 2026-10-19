# Warm-set read boundaries

Some reads returned different keys or writer details after the data entered memory. A page could also end before later matching records. These changes give memory and disk reads the same selection rules.

## Changes

- Narrowed reads carry an explicit range form. Exact caller keys such as `00000061` retain their original range and selected physical tip.
- Named-layout selection precedes the result window. Bounded candidate batches retain their last examined key before value fetch. The primary tip set controls projection fallback.
- Hash and HashRange fields retain each selected tip's writer. The molecule-wide writer helper remains specific to Single fields.
- Each positive memory tip consumes the shared call's key and metadata allowance, including tips rejected by visibility checks.
- Cross-hash `PageAfter` receives an explicit refusal. Administrative pages use offsets and advertise no unsupported cursor. Fixed-hash query cursors retain their current route.

## Validation

Compiler, lint, release build, source review, and synthetic product checks remain pending.

The product comparison uses generated data in owned temporary homes. It uses the normal API and existing operation helpers. No test suite or new executable exercise helper forms part of this work.

Normal local API writes use the node's writer. A different-writer example therefore receives source review and compiler checks only. Ordinary empty-key reads cannot prove that different-writer case.

The separate byte-limit design remains separate work. The primary, its archive, migration, cloud activation, installation, and upgrade remain outside this change. No throughput improvement follows from these correctness fixes.
