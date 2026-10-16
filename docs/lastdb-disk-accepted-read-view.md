# Complete native query views

A native query captures one complete disk root and one optional accepted batch together.
The runtime publishes that pair under one lock.
Every child query shares the same root, accepted batch, read budget, and metadata snapshots.

An exact read uses this order:

1. The current write scope's staged record.
2. The accepted record captured by this query.
3. The captured disk root.

A tombstone is a selected record and hides the older disk value.
A hash range merges the same sources in key order.
The current write scope takes precedence when both memory sources contain the same key.

The accepted batch stays immutable.
A complete disk publication removes the runtime's accepted reference and installs the new root together.
An older query retains its own batch reference until that query ends.
The batch owner must also retain its logical-record protection until the last query reference ends.
The write actor must not remove the batch's records when it transfers bodies to the journal.

Accepted bytes never enter the fetched disk cache.
Queries with an accepted batch bypass that cache and keep their captured bytes separate.
Queries without an accepted batch retain the existing frontier and write-epoch checks.

Typed schema metadata also belongs to the query view.
The metadata snapshot uses the cache identity and schema name as its key.
It accepts at most 10,000 entries and ends with the read context.
The native schema reader must load its first value through the captured store scope.
It must not obtain that value from an unversioned live schema cache.
Write scopes retain their separate staged metadata path.

These interfaces do not activate memory acknowledgements by themselves.
Complete resource admission, the accepted write actor, and native schema snapshot use must integrate first.
The final generated-node proof must cover read-after-ack, concurrent complete queries, and normal reopen.
Source checks alone do not establish those results.
