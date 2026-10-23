# Independent safety source review

The reviewed product source is `bb7a4a7f5219265cb660ba8084b72fdbb2833554`.
The independent product build uses the frozen source `43ca120feb20ae6b95ab36dd3e2a088366522369`.
The build receipt records the tree, command, compiler, binary sizes, and binary hashes.
The later source changes below do not change the native record format or the copied record bytes.

## Integrity and lifetime checks

- `maintenance/catch_up.rs` visits recent directory and locator pages, then filters record versions by the previous batch frontier.
- An edited page can contain old keys. The batch filter keeps those keys from a false delta.
- A new hash group uses the current locator root. The dependency closure retains its locator page-zero template.
- Tombstones pass through the same locator version path. A deleted key cannot return through the copied older base.
- `maintenance/tier.rs` validates the page age and tree level. It rebuilds external keys when it emits a recent page.
- `maintenance/concurrent.rs` holds the source and each captured cut until the complete catch-up round ends.
- `ActivePack` retains the writer lease and excludes an orphan sweep throughout copy, cancellation, and publication.
- Private pages use the next age after their source or captured cut. Final publication allocates the receipt generation at the swap.
- `snapshot/maintenance.rs` performs the bounded final catch-up before it sets the poisoned state.
- A publication error keeps the writer usable only if the on-disk root still equals the installed root.
- `snapshot/reclaim.rs` captures a protected generation before it releases the foreground gate.
- The reclaim path uses the protected closure and the earlier deletion floor. It does not delete the source of an active reader.
- The runtime queue owns accepted prefix work and its quotas. Private prefix definitions do not enter the shared cache before publication.
- The foreground and maintenance publication gates drain accepted prefix work before the final cut.
- The detached maintenance owner continues after the HTTP caller closes its connection.

These are source checks. The product receipts provide separate evidence for the exercised paths.

## Accepted review corrections

The scale reviewer found three resource problems. This review corroborated the source paths and accepted the fixes.

1. `vendor/laststore/src/disk_pack/maintenance/tier.rs:130` parks the output allocator after each tree.
   The old code could retain one file descriptor per touched locator object until copy completion.
   A full part receives its file barrier before closure. An unfinished tail reopens with its exact length check at the final barrier.
   This correction is commit `c1413d3d06256f3a37b84d73eed39a18a94d6136`.
2. `vendor/laststore/src/disk_pack/maintenance/tier_quote.rs:12` charges recent output pages and their external key chains.
   A recent tree page can reuse an old key descriptor. The copy rebuilds that chain, so recent source part size alone is insufficient.
   The quote uses descriptor lengths and receipt ages. It opens no older key chain or payload.
   It includes directory keys, locator keys, internal separator copies, and repeated empty roots.
   It retains the prior metadata slack and payload quote. The checked directory-value conversion fixes the compile-only width mismatch.
   These corrections are commits `d5985afd0e98aac1777dcb9252fdc7186c8b1ed1` and `c11e6ad8c51a4147f211c380e4521c59bd7a4708`.
3. `fold_db/crates/core/src/storage/disk_pack/parts.rs:201` bypasses growth suppression at the soft limit.
   `parts/worker.rs:232` also bypasses the held state there. `parts/worker.rs:335` selects a full copy there.
   The old state could refuse writes after a successful copy and then wait for growth that those refusals prevented.
   `parts/retry.rs:10` owns a non-interruptible exponential delay of 2 through 60 seconds after an over-limit result.
   Refused-write notifications cannot bypass that delay. The delay holds neither the write gate nor the maintenance gate.
   A result below the soft limit resets the retry counter. The final correction is commit `bb7a4a7f5219265cb660ba8084b72fdbb2833554`.

The descriptor correction changes file lifetime. The quote correction changes reservation size.
The worker correction changes maintenance selection and retry timing.
The frozen-build integrity evidence therefore transfers to these source changes.
The parent reviewer owns the final current-source build, product operation, and CI gates.

## Limits

- This source review finds no unresolved integrity defect in the reviewed scope.
- The product operations use fresh generated homes. They never use the primary or its archive.
- A process interruption does not establish a hardware power-loss result.
- The operations do not cover every receipt or root-publication interruption window.
- The operations use inline native bodies, including a 128 KiB body. They do not use external file blobs.
- The physical 85 percent transition and an exhausted disk reservation remain unproved by this review.
- An ordinary automatic worker result does not establish the physical 85 percent transition.
- The recent-tree quote and the over-limit retry conclusions rely on source checks for their boundary cases.

No test, fixture, primary database operation, install, daemon restart, or shared infrastructure change forms part of this review.
