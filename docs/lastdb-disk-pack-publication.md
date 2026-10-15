# Separate artifact root publication

The disk-pack module can publish complete journal batches in a separate artifact.
The primary, the archive, and the reset stores are outside this command's scope.
The LastStore home bridge and the memory ack do not use this publisher.

Use the installed candidate program from its source worktree:

```text
lastdb-journal publish --artifact /explicit/generated/artifact
```

The command acquires the artifact journal's exclusive lock.
It captures CURRENT and TAIL before it reads a complete suffix.
A live artifact writer causes a lock refusal.
The command does not admit concurrent artifact mutations.
Online writer coordination remains separate work.

The command first creates or verifies the current generation receipt.
The receipt keeps the complete original CURRENT bytes and its complete live dependency list.
The command then verifies each complete journal batch through the captured TAIL.
It sorts that batch's physical work by collection, group, key, and mutation version.
This order preserves the last operation at each coordinate.

Each locator change copies only its tree path into fresh private parts.
A leaf split divides at most 54 entries into two leaves.
An internal split divides at most 90 children into two internal pages.
Every completed non-root internal page retains at least eight children.
A root split can add one level, within the eight-level format limit.
Tombstones remain versioned locator entries.
The writer never modifies, appends to, or removes a published metadata part.

Fresh metadata parts use up to eight exact create-new attempts with random `u32` part numbers.
Part zero remains reserved for initial tree construction.
A long-key suffix reserves its complete contiguous chain before its first page.
No directory walk finds a free part number.
Every copied path retains its tree UUID and every unchanged child page reference.
An error poisons the private tree edit and prevents its root publication.

The publisher completes locator file and directory barriers before the directory root can reference them.
It completes the directory parts before the generation receipt.
It verifies the previous generation receipt and its named immutable parts.
It adds every newly created tree part and every complete new journal object's parts.
The builder records the initial parts as it creates them and writes the initial receipt before CURRENT.
Normal publication does not walk every key to discover dependencies.
It records directory, locator, overflow, block-map, journal-data, and pack-data parts.
The receipt uses Appendix A4's kind-11 pages and exact 80-byte dependency entries.
Whole-part checksum flags remain zero in these receipts.
Page and frame checksums remain mandatory; a future backup must supply its own complete integrity proof.

The receipt file and directory barriers precede the CURRENT rename.
The replacement CURRENT has its own file barrier before the atomic same-filesystem rename.
The artifact root directory barrier completes the publication.
A changed CURRENT under the exclusive lock causes a refusal.
An error after the rename is an uncertain publication result and requires recovery.
The command never rolls CURRENT back after that error.

All old files and the old generation receipt remain in place.
The new receipt conservatively retains prior dependency parts and private intermediate tree parts.
Its list can therefore contain parts that the new directory no longer reaches.
This unit performs no physical reclamation.
It does not yet supply a durable backup reference, a rollback command, or a restore command.
The proposed receipt is a dependency for those commands.

The journal TAIL can retain a prefix that CURRENT already includes.
Recovery starts at `CURRENT.frontier + 1` and never replays that prefix.
It still verifies the captured final marker and the applicable suffix byte bounds.
When CURRENT reaches TAIL, a repeated publication verifies the generation and performs no root change.
Journal resume and stream rotation remain unavailable.

Administrative bounds are explicit:

- At most 65,536 distinct dependency parts.
- At most 262,144 metadata pages and 1,000,000 metadata entries for an older artifact's initial receipt adoption.
- A finite 4,096-page and 256 MiB read budget for each tree path or metadata page's work.
- The existing complete journal suffix and batch admission bounds.

A limit causes an error before CURRENT publication.
It does not mean a record is absent.
These artifact-command bounds do not change the warm set or impose a new node storage cap.
Only an older artifact without a generation receipt needs the initial administrative closure walk.
That walk does not reconstruct record bodies.
Normal publication uses the held receipt and exact new part identities.
Its manifest work scales with retained part count, rather than stored key count.
A future compaction must rebuild the live dependency list before retained-part growth reaches the command's explicit limit.
Online admission, stream resume, and durability-token coverage remain necessary before activation.

The separate `lastdb-pack read-batch` command accepts exact collection, group, and key coordinates as JSONL objects.
It uses one captured CURRENT and one shared call budget for the complete batch.
The existing `read` command retains its single-collection input format.
Neither command admits storage objects to the warm set.
