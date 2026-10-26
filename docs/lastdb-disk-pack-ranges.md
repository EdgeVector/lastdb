# Bounded ranges in separate disk-pack artifacts

The artifact reader supports an ordered range under one encoded hash.
The caller supplies the collection, complete encoded hash prefix, and its configured groups.
The prefix ends at the first NUL separator after the complete hash.
The group list contains one through sixteen distinct groups, each below 1,024.
The LastDB integration must obtain the scope and groups from its held codecs.
The artifact coordinates and codec defaults do not allocate a production codec identity.

The range bounds contain encoded range suffixes.
The start is inclusive and the end is exclusive.
An absent end means the exact end of the held hash.
The reader cannot construct a range across a second hash.
It refuses oversized bounds before it allocates their combined keys.
Equal bounds return an empty range without a tree seek.

Each configured group resolves through the captured directory root.
A group with a valid directory miss contributes an empty cursor.
Each present group seeks its locator tree with full-key comparisons.
The cursor keeps an ancestor stack with at most eight levels.
It uses the parent stack to find the next leaf.
It never depends on sibling links that can become stale after a path copy.

The reader merges at most sixteen pending keys in the existing byte order.
It verifies key order, parent intervals, location versions, and complete record identities.
A duplicate full key in two configured groups causes an integrity error.
The reader does not guess which physical placement is correct.
Tombstones remain examined entries and do not appear in the result.
Every live value uses its exact committed payload extent.
Ordinary range reads perform no journal replay or hash-group reconstruction.

The in-process cursor borrows its original ReadCall.
Every page shares that call's captured CURRENT, page cache, block cache, and total budget.
The type prevents a new call from resetting the cursor's budget or generation.
It also keeps the call alive while the cursor retains storage objects.
All cursor and call objects release their buffers when their scopes end.
This API supplies no external continuation token or new token codec.

The call counts every examined key, including tombstones and pending keys from other groups.
It also counts a key that establishes the upper boundary.
A small result limit does not remove the need for the bounded group lookahead.
The existing page, block, byte, and key ceilings remain in force across every page.
A budget error poisons the cursor and returns an error.
It never returns a short successful page or a false exhausted result.
A short successful page always exhausts the requested range.
A full page can conservatively report that more work remains.

The disk format v1 removal deleted the `lastdb-range` product command on 2026-10-26.

The LastDB query routes do not use this API yet.
The warm set retains its existing 10,000-logical-record path.
This unit does not establish local-write version checks or warm-set admission for the new format.
Those steps remain necessary before home activation.
The primary, archive, factory, and reset stores remain outside this command's scope.
