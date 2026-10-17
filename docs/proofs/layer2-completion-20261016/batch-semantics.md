# Batch semantics: b47 component evidence

This historical component proof uses source `b47eb21cc20bf1363482e386c2e79b320f0c9bac`.
It does not establish the final candidate's complete write or recovery result.
The final source uses a separate `semantics-7110.md` report after its product checks complete.
The daemon version is `0.23.4-pre.0-244-gb47eb21cc`.
Its SHA-256 is `37c362262c5ac2854ec2143278dc7df31fc32936a72abdbdc643b7b216d79880`.
The node uses native write mode, a 500 ms flush interval, and a 10,000 logical-record cap.
The home starts empty. All 256 records use generated values. No primary data enters this proof.

- All 12 complete response comparisons match the full JSON, including field metadata, the added bucket field, key form, and page values.
- The repeated 1-, 16-, 64-, and 256-key reads show zero file opens, read operations, payload bytes, and metadata operations.
- The fifth request shape also reaches zero file reads after one normal read. Old request aliases cannot block a new shape forever.
- Alias, field projection, and descending-order reads retain their complete response shapes. Their repeated reads also show zero file reads.
- An absent key returns 255 records. A page limit of 64 returns 64 records with `has_more: true`.
- A later full request returns all 256 records with the same JSON as the earlier full request.

The critical partial-memory case starts with the earlier 256-key results in the warm set.
A durable batch then updates 64 records. A separate read fetches only 16 current keys into the warm set.
The first subsequent 256-key request returns HTTP 200 and all correct updated values. It does not need a retry.
The next identical request matches its full JSON and reports zero file reads.
The request uses `HashRangeKeys`; a new key set or a range can use the normal fetch path.

The 64-write receipt contains 64 update IDs. The feed returns those same IDs and keys in the exact input order.
Its sequence numbers run from 257 through 320 without a gap. Its final cursor is `v1-0000000000000140`.
The native frontier changes from 50 to 53. This is one author-clock publication, one data publication, and one feed publication.
The generation changes from 102 to 108; it is an identity, not a count of publications.
This component run reports 7.186 seconds for the ack stage and 24.572 ms for preparation. It is not a quiet benchmark.

The explicit flush returns durable. Both normal stops exit with code zero.
After the normal reopen, the full 256-key response matches the pre-stop JSON.
The complete 320-event feed also matches the pre-stop JSON, including its order and final cursor.
The ordinary status sample reports 1,024 logical records against the 10,000 cap, with zero held or excess records.
This sample does not prove behavior at a full cap.

The earlier source `511b8982848fddf62457ce5953a57ee80fa6e4d0` exposes a real defect.
Its first post-update 256-key response returns HTTP 503 with `read resource limit exceeded`.
Optional copies of request descriptors and wire metadata consume the same cumulative call budget as the necessary data read.
The b47 source checks complete warm coverage before it copies field data. It declines optional metadata when the remaining budget cannot hold it.
The b47 source also removes prior optional metadata copies from required field admission. The real data-read limits remain strict.
The refused response and its complete read receipt remain under `batch-511b-post-update-256-refused.*`.

The earlier 511b component run also checks mutation behavior. A conditional write succeeds; the stale condition returns HTTP 409 with `cas_conflict`.
Three writes to one key share the existing batch clock. The largest mutation UUID selects `repeat-B`.
A delete removes one record. A create with the original bytes restores it.
A queued write returns while native status reports an accepted batch. An immediate read returns its new value.
The full 256-key response remains equal after flush and normal reopen. These receipts retain their earlier component label.

`batch-semantics-summary.json` contains the comparison results and exact values.
The b47 and earlier receipt manifests contain response hashes and complete read-cost receipts for each saved call.
Selected complete responses and b47 HTTP headers retain the historical `batch-final-` filename prefix.
The input requests are under `requests/batch-final-*.json`; the earlier mutation requests use `requests/batch-511b-*.json`.
Documentation copies of HTTP headers use LF and one final newline. The scratch keeps the original header bytes until cleanup.
No tests, fixtures, failure injection, primary operations, or source changes occur during this product proof.

All owned nodes are stopped. The supported `lastdb-dev reclaim --home` command removed both owned scratch roots after the benchmark interval.
The roots were `/private/var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T/layer2-batch-proof.laajmb`
and `/private/var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T/layer2-batch-final.QKboR6`.
Both commands return zero, and both paths are absent after the command.
`batch-cleanup-results.json` records the exact paths, ownership, stopped process state, and helper receipts.
`batch-scratch-receipts-511b-b47.tar.gz` preserves every raw request, response, header, and log from those roots, excluding the generated database homes.
