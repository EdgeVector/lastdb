Native shared-key cleanup could leave an old key after its lookup value cleared, or remove a key after another source replaced its association. Later writes in one batch could also copy earlier field values. Mapped molecules could return another layout's records through pages, counts, or ranges.

This change uses one current-association proof for related-key removal and one declared-key membership rule for query selection. Create/update groups prepare from the current private state immediately before their stage. Repeated canonical keys start the next stage; independent keys remain batched. The native transaction retains complete reservation and publication boundaries.

The single-publication rule covers create/update batches. Mixed requests publish the create/update batch first. A later Delete or Purge can fail after those writes publish.

Administrative pages recover usable API keys, establish a complete bounded set, and then select the page. Samples stop after enough valid records. Ordinary paged range reads retain body batches of at most 1,024 records. The memory sampler also removes a retired trim call and captures native counts after pressure relief.

Validation:

- Full workspace and separate feature-lane Clippy pass with warnings denied. The normal release build and format, size, new-allow, artifact, registry, write-path, and workspace checks pass.
- Fresh synthetic runs pass 172 query observations across blind/OPE and plain keys, with normal reopens. The results cover nullable keys, shared-key collisions, deletes, purges, moves, dependent writes, mapped layouts, counts, and descending page order.
- The unchanged core write helper passes 74 read observations, including 29 with no file reads and 36 concurrent response states.
- Independent reviews verify the 1,890 source inputs, the release executable, raw responses, and exact cleanup. All owned temporary roots are reclaimed after receipt preservation.

Per-field conflict rules remain. Source cleanup leaves a mixed or otherwise unproved related association unchanged. Cold reads can require the declared range field to prove membership; warm reads reuse the core model. Administrative pages and counts return an explicit error if the existing read allowances cannot establish completeness. This review makes no throughput or latency claim.

See `docs/proofs/native-boundary-review-20261019/RESULTS.md` for source identity, operation receipts, and the two preserved administrative batch refusals. Those refusals came from unsupported request routing; the accepted runs use the supported single-request route in parallel.
