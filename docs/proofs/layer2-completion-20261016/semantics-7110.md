# Fresh synthetic reads and writes

This proof uses source `7110c7f54ea005e33678b5c93e89353d6117b41f`.
The node version is `0.23.4-pre.0-249-g7110c7f54`.
Its SHA-256 is `5a7b18dcb206e46711a07dc497edebdb267867b9073fc4d516518056c82b6244`.

The node starts with one new empty home and a new identity.
It uses native write mode and a 500 ms flush interval.
All data is synthetic. This proof imports no home, copies no home, restores no backup, and does not reopen the home.

- One mutation batch creates 256 records. A batch read returns all 256 expected titles and bodies.
- A repeated read matches the complete JSON, including keys, field metadata, flags, and page values.
- One durable batch updates 64 records. A read fetches only 16 current keys into the warm set.
- The first subsequent full 256-key read succeeds and returns every expected value. Its repeated full JSON also matches.
- A conditional write succeeds. The stale condition returns HTTP 409 with `cas_conflict` and leaves the new value intact.
- A delete makes its exact query return zero records. A create restores the expected title and body.
- A queued write returns a queued receipt. Its next exact read returns the new value.

The queued status sample already reports no accepted batch.
This sample does not establish that the read precedes the flush.
The final full read returns all 256 expected records after every change.
Its repeated full JSON matches, with canonical SHA-256 `690fb9af65f1f9c6aa1d1bd3a5dce48e5c3dea2884b1ffd16aa2006fc8754377`.

The explicit flush returns `durable: true`, frontier 57, and generation 116.
The normal node stop returns zero. No node remains active, and no reopen follows the stop.
The status sample reports 899 logical records against the 10,000 cap, with zero held or excess records.
This sample does not prove behavior at a full cap.

The complete raw requests, responses, headers, and logs are in `semantics-7110-raw.tar.gz`.
The receipt manifest records their hashes. Selected complete responses and all HTTP headers use the `semantics-7110-` prefix.
Documentation headers use LF and one final newline. The raw archive keeps their original bytes.
The input requests also appear under `requests/semantics-7110-*.json`.
`semantics-7110-summary.json` records every comparison and the exact source scope.

This operational proof adds no performance measurements. The separate final benchmarks use fresh synthetic homes for both programs.
Read-cost headers were not requested in this proof. The benchmark report supplies the read-cost measurements.
No tests, fixture suite, failure injection, primary operation, or product source change occurs here.

After receipt preservation, the supported exact-path cleanup removes this generated home and the earlier extra-proof scratch root.
Both cleanup commands return zero. Their paths are absent, and all owned nodes remain stopped.
`semantics-7110-cleanup.json` records the realpaths, owners, process checks, and helper receipts.
