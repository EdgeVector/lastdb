# Layer 2 comparison recipe

The baseline source is `7110c7f54ea005e33678b5c93e89353d6117b41f`.
The new source is `a1d03310e433b99c788657ef149193776bf5aff6`.
The comparison uses saved programs with recorded SHA-256 hashes.
Both use native LastStore storage.

CAUTION: Create independent empty homes. Use generated data. Do not use the primary, its archive, or another user's home.

These are real product command measurements. They are not a test suite or a fixture harness.
Each program receives the same generated keys and values through its own new home.
The commands write the data, read the data, and compare the returned bytes with the input.
The node run also reopens its own generated home after a normal stop.

## Node requests

Use `drivers/node_compare.py` with the saved before and after `lastdbd` programs.
Use resident mode `write`, an unset disk cap, and native storage for both.
Use separate identities and Unix sockets. Leave cloud configuration absent.
Seed the schema through the isolated system-schema route. Do not call schema service.

The schema is `Layer2QueueBenchmark`.
Its hash field is `bucket`; its range field is `id`.
Its fields are `bucket`, `id`, `title`, and `body`.
The field descriptions are identical in both programs.
Create 320 records through five durable requests of 64 records each.

Record N uses hash `generated` and range `record-NNNNNN`.
The title is `title-N-revision-V`.
For the body, concatenate 16 lowercase SHA-256 hexadecimal digests of `Layer2Queue:N:V:chunk`.
The chunk starts at zero. Each body contains 1024 ASCII bytes.
The initial revision is zero.

For each read case, request keys 32 through 31 + C.
Use counts C of 1, 16, 64, and 256.
An exact request uses one `HashRangeKeys` selector with all C keys.
A range request uses one hash, an inclusive start, and an exclusive end.
Prime each case, then make 21 measured calls per program.

Alternate program order on each measured round.
Compare each full response with its prime while no write occurs.
Check values, order, counts, hash keys, completion flags, and page details.
Request the product file-work receipt on every query.

For each write size, make five durable update requests and five queued update requests per program.
Each request updates `title` and `body` for all C keys.
Use distinct revisions for each size, mode, and round.
Record the returned durability guarantee separately from the requested guarantee.
After the response, read native status, call explicit flush, and read native status again.
Record the acknowledgement duration and the request-start-to-durable-response duration separately.

The new flush receipt states its captured accepted sequence and completed sequence.
Check that the durable frontier advances and the writer reports no failure.

For the mixed case, prime exact keys 32 through 95.
Update keys 304 through 319 through one durable request, then flush.
Repeat the original 64-key query with a file-work receipt.
The user values must remain equal.
The molecule version can change because the write uses the same fields.
Use five alternating rounds per program.

For the concurrent case, prepare four requests of 16 disjoint keys each.
Prepare the generated bodies and JSON before the shared start signal.
Release all four calls together. Each requests a queued guarantee.
Record each response time, the last response time, native status, and the explicit flush receipt.
Use five alternating rounds per program.

Keep thread setup and result collection outside the acknowledgement interval.
Record the complete wall time separately.

Delete 16 keys through one queued request, then read the affected keys before flush.
Flush and read the bounded range. Recreate those keys and repeat the reads.
Read all 320 explicit keys after the complete change.
The other records still reference shared field atoms during this operation.

For three normal-flush observations, request queued updates of 16 records.
After each response, poll native status every 20 ms. Do not request a flush during the observation.
The first queue-empty observation gives an upper bound on disk completion after the acknowledgement.
Later feed work can extend this bound.
Stop each owned node normally, reopen its generated home, and check all 320 known keys.

## Disk commands

Use `drivers/engine_bench.py` with the saved before and after `lastdb-disk-home` programs.
Create independent homes with fanout 1 and an unset disk cap.
The input has 4096 explicit keys in collection `Layer2Receipt`.
Key N is `generated`, one NUL byte, then `record-NNNNNN`.

For body size S, concatenate SHA-256 digest bytes of `Layer2-Oct16:N:chunk`.
Start chunk at zero and truncate to S bytes.
Use an empty body when N modulo 997 is zero.
Use a deleted record when N modulo 251 is zero; this rule takes precedence.
Use body sizes 128, 1024, and 16384 bytes.

Use 31 rounds for 1024-byte bodies and 11 rounds for the other sizes.
Alternate program order on each round.
Use batches of 1, 16, 64, and 256.
Read consecutive keys, scattered keys, and repeated keys with absence.
Read one bounded range page at each size.
Write distinct keys and repeated keys in complete ordered batches.
After the final write round, read all known keys and compare the bytes.
For 16384-byte bodies, use 1024-key verification calls within the normal read budget.

The many-map case uses `drivers/engine_many_maps.py`.
It uses 256 collections with 16 keys each, the same body recipe, and 1024-byte bodies.
Every 64th key has a 193-character suffix, after a hyphen.
These 64 long keys exercise page references across separate maps in a shared file.
One 256-operation update touches every collection. Use 11 alternating rounds.
Read all 4096 explicit keys after the changes.

## Compaction and interpretation

Use the saved `lastdb-pack-maintain` program for each variant.
Run it only after the corresponding owned node or command writer stops.
Create a backup reference, compact, and attempt reclaim while the reference remains held.
Release that reference, then reclaim again.

Compare the complete ordered record digest before and after compaction and reclaim.
Record logical file lengths, allocated bytes, and counts by file kind.
Keep each inventory within its exact generated home, depth six, and 262144 files.

The operating system cache state is unknown. A fresh process does not establish a cold device.
File-work counters measure program calls, not physical device traffic.
Node times include the Unix HTTP call and response decode.
Disk times use the product clock; process duration is a separate receipt.

The p95 uses nearest rank. A sample count below five has no p95.
Both owned nodes remain active during the matched node matrix. Calls alternate between them.
Normal product feed work can continue after a request.
No other team-owned node or build runs during the timed matrices.
