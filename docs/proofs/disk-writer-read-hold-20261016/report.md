# Native writer-owned read hold receipt

Date: 2026-10-16. Source: `4b689af8`.
The release product commands use a new home with 32 generated records.
The commands use no primary data, archive, node, or tests.

`lastdb-pack-maintain hold-writer --hold-seconds 20 --grace-ms 0` reports zero file opens, reads, format-probe bytes, and metadata operations for the hold.
The command also reports a deferred internal reclaim.
It then drops its journal handle and capability, and keeps only the active generation hold.
A second product process cannot acquire the exclusive writer lease during that hold.
Its exit code is 1, with `Resource temporarily unavailable (os error 35)`.

After the hold ends, the second process acquires the writer lease and completes reclaim.
It removes two obsolete receipt files and 32768 bytes.
The administrative record stream returns all 32 keys and their expected generated body hashes after reclaim.

These commands prove the in-process hold protocol on this generated home.
They do not measure a native query, primary performance, or power-loss behavior.
The reader integration has a separate product check.

Validation: package Clippy with warnings refused, release product builds, format, and source size checks.
The scratch home remains retained for review; no process or hold remains active.
