# Packed disk component operations

These receipts come from fresh generated disk homes. They do not use Mini schemas or a previous LastDB home.

The product commands built each home, read keys, read ranges, changed records, and compacted the files. The final commands reopened the files.

| Home | Initial records | Stored key bytes | Result |
| --- | ---: | ---: | --- |
| ordinary | 4,096 | 174 | Build, read, update, delete, insert, compact, reopen |
| boundary | 512 | 1,024 | Build, read, update, delete, insert, compact, reopen |
| overflow | 128 | 1,025 | Build, read, update, delete, insert, compact, reopen |
| mixed | 2,048 | 174, 358, 1,024, 1,025 | Build, read, update, delete, insert, compact, reopen |
| tall | 20,000 | 1,024 | Build, read, 256 wider inserts, update, compact, reopen |
| max-key | 3 | 1,024, 1,025, 65,535 | Build, exact read, complete range read |

The ordinary complete range uses 16 batch file reads for 4,096 records. Its keys use no overflow pages.
The 1,024-byte keys also use no overflow pages. The 1,025-byte keys use one overflow page each.
The 65,535-byte key uses five overflow pages. All six homes return the requested records.

The first four homes use an earlier parent-page occupancy choice. The final program reads all six homes with the same packed format.
The tall and max-key homes use the final parent-page choice.

These elapsed times are component observations. They are not the before/after Mini benchmark.
Other synthetic work ran on the machine at the same time.

The JSON files retain the command receipts and the raw read costs. `executables.sha256` identifies the final component programs.
`preservation.json` retains input file hashes, the exact scratch path, and the cleanup check.

The scratch path receives its ownership marker after the exact path and file checks pass.
The first cleanup preview refuses the live creator process. The next preview permits cleanup after that process exits.
The supported exact cleanup removes the scratch path. `cleanup.json` records the result.
The open-file check finds no live user of that path. No cleanup bypass runs.
