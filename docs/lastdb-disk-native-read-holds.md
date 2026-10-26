# Native read holds

CAUTION: This page records the October 15 implementation step.
Its fetched-storage-record cache and unfinished-work statements describe that step, not the current logical-path warm set.
Use the [current source status](lastdb-disk-rollout-status.md) for the present boundaries and receipt links.
The [7110c7f5 node receipts](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/layer2-completion-20261016/node-final-off-summary.json) report the later candidate's measured costs.

The native fetched-record cache uses the existing 10,000-key budget.
The previous path could purge a fetched record while another call still held its returned bytes.
It also returned an uncached value when every cache entry was protected.
This change reserves each exact key for the life of its native read scope.

## Hold and release

An exact read reserves its coordinate before the disk fetch or memory copy.
The shared read scope takes one hold for each distinct collection and key.
Child query items share the same holds when they share the same read root.
Separate active scopes take separate holds on a shared key.
The budget counts the key once while any scope holds it.

A new key takes a slot before the fetch.
The cache first purges unheld, clean records in least-recently-used order.
If no slot is available, the read returns an explicit resource error.
It does not return an uncached success or an absent result.

A range takes a hold before it copies a pending body or returns a decoded disk record.
The directory reader still bounds its temporary pages and decoded blocks separately.
Those temporary objects are not warm entries.

A write invalidates a fetched value but keeps its held slot until the older call returns.
A later fetch can fill that slot without a second budget charge.
An invalidated value cannot become a warm hit for a newer root.
The existing root and write-epoch checks still control cache admission.

The last owner of the read scope releases all holds in one operation.
Clean fetched values remain warm.
A failed or invalidated fetch with no cached value releases its reserved slot.
The current held-key gauge includes these exact-record holds.

## Limits

The change protects native fetched storage records, including addressed absences and internal metadata.
It does not establish the complete schema–field–molecule–atom admission contract.
It does not change dirty-write admission or grant memory-first acknowledgement authority.
The older typed resident paths and their full protection gates remain open work.
The 10,000-key budget is not a process memory-size limit.

Native resident-first modes and cloud-connected native homes remain refused.
The default backend and primary remain unchanged.
