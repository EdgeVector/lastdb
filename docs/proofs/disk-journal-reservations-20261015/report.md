# Journal reservation receipt, October 15, 2026

Source: `b3157a87e08484b2aa251cb091deea4e9d98b406`.
Release home command SHA-256: `850d90d366e6b868c9c0ae22aa291856de316cd6b78c71a9a0519a69af573579`.
Release journal command SHA-256: `fbcc69f8c034f5cea708d2961f404f65ec200e5039b1f84c0478c2a58781dc84`.

The product commands use the generated 25,600-record home from the generation-carry receipt.
No primary, primary copy, archive, factory, shared service, test, fixture harness, or failure injection participates.

The `plan` command calculates 32,448 logical bytes and 49,600 worst-case physical quota bytes for 192 operations.
It opens only three fixed files and reads 12,288 format bytes. CURRENT and TAIL retain their digests.
The result explicitly refuses directory and logical-record reservations and a memory ack.

An ordinary append holds one 49,600-byte ticket before I/O.
After append, the ticket count and reserved bytes return to zero; the durable suffix owns all 49,600 bytes.
The readable frontier stays 3 while the complete durable batch reaches 4.
The prior compatible reader still returns all 384 pre-publication results.

The next ordinary write recovers complete batch 4 before it adds batch 5.
After directory publication, both ticket and durable-suffix quotas return to zero.
The prior reader returns all 385 expected slots: 192 absences, 64 unchanged bodies, 64 admitted bodies, 64 reserved bodies, and one new body.
All 193 initial data and metadata parts retain their SHA-256.
Ordinary reads report zero group opens, legacy replay, journal replay, footer bytes, and directory walks.

The plan command also receives 4,097 operations, then a stream of 128 one-MiB bodies.
Both exceed a complete batch limit. Both commands exit 1 with zero output bytes and an explicit limit error.
No writer opens for those plan requests.

Package and workspace library/binary Clippy, release command builds, format, diff, source-size, function-size, new-allow, and non-test lints pass.
The source defines the two-second materialization recommendation and five-second admission refusal with monotonic time.
These operational calls do not establish the aging boundary, uncertain-I/O behavior, or all concurrent ticket cases.
No test command or fault injection runs.

The journal quota is one component. Filesystem space, directory output, complete prepared schema envelopes, protected logical records, and native memory acknowledgements remain separate gates.
Conversion, backups, rollback, and final performance also remain open. No primary upgrade occurs.
