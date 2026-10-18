# V5: preserved performance failure

This run compares accepted completion `a0ad6190…` with candidate `8a26322c…` from source commit `dec63731…`.
All eight cases complete. Every case uses a fresh synthetic home.
The full [measurement tables](TABLES.md) remain unchanged.
This candidate fails the performance review and does not serve as the final result.

Warm HashRange updates take 2.63 times the reference ACK latency with preload OFF.
They take 2.91 times the reference ACK latency with preload ON.
The [ACK observations](WRITE-FRONTIERS.md) record 16 model allowance refusals across those 20 updates.
The other candidate write groups record no model allowance refusal.

In current-on round 1, the first warm update leaves 11,861,056 metadata bytes before the flush.
The flush leaves 5,430,784 metadata bytes in the current core model.
The next update reaches a 16,761,640-byte peak under the 16,777,216-byte cap, then records one refusal.
Each later update records another refusal. Their ACK observations show no pending batch and no dirty logical records.

The API still reports `queued` for each write.
That response does not establish that the ACK precedes the disk write.
Status follows the response, so a caught-up status alone does not establish the internal ACK path.
Here, the allowance counters and source review identify the disk-first fallback.

Source review finds a duplicate metadata charge for strings that the logical record already owns.
The molecule stores references to those records. Its preparation also copies the source lists to reserve capacity.
The next candidate removes that duplicate charge and those temporary copies.
It keeps the same 16 MiB metadata cap and reservation ownership.

No CPU profile starts. The retained counters and source review identify the cause without another node.
The final candidate requires a fresh operational matrix and all eight matched cases.
The [raw-derived JSON](write-frontiers.json) retains every request ID, response phase, and before/after status.
