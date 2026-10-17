# Final map pages per complete batch

The journal keeps every accepted operation in its original order.
Publication sorts only the physical map work after it verifies the complete batch.
Repeated coordinates select their final physical mutation version for the map.
The journal remains unchanged.

The publisher collects the final changes for each locator tree.
One recursive tree edit visits each affected old page once for that complete batch.
It merges the changes into each affected leaf and writes only the final leaf pages.
It then writes the final parent pages, with the existing split and occupancy rules.
Unchanged children retain their exact immutable page references.
The directory uses the same complete-batch edit after the locator roots are durable.

Every output page uses a fresh page identity in a private part.
The writer does not alter a published page, part, or checksum.
The existing file barriers, generation receipt, CURRENT publication, and directory barrier stay in place.
A failed edit remains poisoned and cannot publish a root.

The saved format and reader APIs do not change.
The 4,096-operation batch ceiling also bounds the collected final map changes.
The existing metadata work budgets still constrain publication reads.
Separate journal batches retain separate map updates and complete mutation boundaries.

This change reduces intermediate private map pages within one complete batch.
It does not add physical reclaim, full write admission, or memory-first acknowledgements.
Native resident-first modes remain refused.

Package library and binary compilation passes.
Format, diff, function-size, file-size, and new-allow source checks pass.
Generated-data product measurements remain required before a speed or activation claim.
