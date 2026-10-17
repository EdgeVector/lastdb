# Earlier benchmark work

These results do not describe the final 7110 program.

- [08409 physical component](prior-08409a220/README.md): all three matrices and the earlier 101-round batch-16 follow-up.
- [Earlier D1 report](historical-d1fae6469/report.md): the first physical measurements and their tail regression.
- [Old native node samples](node-off-08409-intermediate-summary.json): valid baseline reads and writes through 64 remain the before figures.
- [Old 256 workload limit](node-off-08409-intermediate-status.json): the old writer becomes unhealthy; its normal stop exceeds 120 seconds.
- [b47 refusal](node-b47-off-intermediate-status.json): a 256 update reaches the internal read budget; normal reopen retains all 320 prior values.
- [ef1f refusal](node-ef1f-refusal-status.json): typed internal key-budget refusal after the first preparation changes.
- [2098 diagnostic](node-2098-refusal-status.json): numeric stages reach the old key limit; normal reopen retains all prior values.
- [9a diagnostic](node-9a1c-refusal-status.json): the larger read budget reaches the separate 4096-operation format limit.
- [Excluded preparations](excluded-node-preparations.json): initial setup and drain limits remain separate from completed latency samples.

The b47 status also preserves the benchmark parser error and the socket-path preflight error.
Those driver mistakes are excluded; they are not product refusals.
The old failed generated home and the intermediate failed homes remain intact for review.
No primary or archive data participates.

Before the user's later instruction, one [read-only format check](format-compatibility.json) also runs on generated benchmark data.
The 9a reader refuses feature 0x20; the current reader reads the earlier 31-feature generated home without a header change.
That check is ancillary history. Backward compatibility is not a completion requirement.
No further compatibility or existing-home recovery load follows the later instruction.

The [supported cleanup attempt](benchmark-cleanup.json) refuses the unstamped generated home: `exact seed owner or mode differs`.
No ownership stamp or manual deletion bypass follows. The scratch files remain intact for review.
