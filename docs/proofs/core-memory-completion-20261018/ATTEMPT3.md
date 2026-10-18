# Completion attempt 3: remaining default-route cost requires a CPU profile

The third completion build passes the unchanged generated API proof and all eight matched measurement cases.
The default route still takes twice the previous build's time for repeated bounded ranges. The merge remains blocked.

The candidate executable is `5f568d9956667b20125c2db5e9716d04b15c5bbccd9535dc065c171de3956cf8`.
[attempt3-identity.json](attempt3-identity.json) preserves its 1,908 product and build inputs.
The local source commit is `827f9f60417515bda6f0f49ecc8131c6fab79ef1`, tree `c68c68ded4fd5490ab88429675ce96f553bacd48`.
The comparison reference remains reviewed v4 executable `bdb83b38545a2161217392f19f9e6f6c2e0fd2d35f8f449e2d6221cb29f2cbed`.

The ordinary API proof passes 219 keyed memory-state checks and 30 equal-content checks.
It passes 58 zero-file-read gates and 24 additional warm equal-content gates.
The same-key allowance stays equal through 24 create, delete, and explicit-flush cycles.
All nine runs stop their 139 owned node sessions normally.
See [operational result](operational-attempt3/result.json) and [receipt seal](attempt3-receipt-seal.json).

All 696 benchmark read checks and 112 measured write batches pass.
The case order and fixed generated data remain the same as attempt 2.
Each repeated median uses ten observations. First and disjoint medians use two.
The operating-system file cache remains uncontrolled.

Repeated read medians, milliseconds:

| Read | v4 off | Attempt 3 off | v4 on | Attempt 3 on |
|---|---:|---:|---:|---:|
| Hash exact, one key | 0.279 | 0.292 | 0.297 | 0.316 |
| HashRange exact, one key | 0.286 | 0.322 | 0.347 | 0.366 |
| HashRange bounded, 64 records | 3.984 | 7.955 | 4.574 | 5.042 |

Read-primed HashRange write ack changes from 180.895 ms to 229.627 ms off, and 184.050 ms to 231.574 ms on.
The matching through-flush medians are 501.919 ms to 533.142 ms off, and 509.914 ms to 531.283 ms on.
All costs, individual observations, write conditions, and limits remain in [comparison.json](attempt3/comparison.json).

The later CPU profile uses a separate fresh generated home, after every timed case stops.
The profile identifies API key encoding in `note_selected_query_tip` before that call finds no physical source receipt.
The next change retains the actual location first and avoids that encoding for body-only reuse.
Actual physical receipts retain their full candidate checks. Body reuse does not create lookup authority.
All attempt 3 observations remain diagnostic evidence. They are not a final performance acceptance result.
