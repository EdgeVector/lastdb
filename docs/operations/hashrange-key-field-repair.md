# HashRange key-field repair

Use this repair when a HashRange partition has sparse declared key-field
molecules. The repair targets one schema and one API hash.

The declared range-field molecule supplies the partition member list. The
repair does not walk other schemas or hash partitions.

Run a dry run first:

```sh
lastdb db repair-hashrange-key-fields \
  --schema <schema-name-or-identity> \
  --hash <api-hash>
```

Review `rows_planned` and the three before counts. Then run the same bounded
repair with `--execute`:

```sh
lastdb db repair-hashrange-key-fields \
  --schema <schema-name-or-identity> \
  --hash <api-hash> \
  --execute
```

The command is owner-only. It uses normal synchronous mutations in pages of
64 rows. Each mutation writes both declared key fields through the atomic
field batch.

## Clone proof for `repo=brain`

The proof used an isolated CoW clone. It did not write to the primary node.

- Schema identity: `17a37bbceed9d4f4c62d1836d6d70919d4a98ea6dcf5ea1fe15304b854a2a6b8`
- Descriptive name: `LastgitRepoPolicyEventV2`
- API hash: `brain`
- Declared hash field: `repo`
- Declared range field: `name`

The initial dry run reported these distinct live row memberships:

| Field | Rows |
|---|---:|
| `repo` | 1 |
| `name` member list | 508 |
| `oid` payload | 508 |

The execute pass planned and wrote 507 rows. Its post-write report showed 508
rows in `repo` and 508 rows in `name`.

The database then closed and reopened. Two new dry runs produced byte-identical
JSON with SHA-256
`68393c8f2d5485b9ad402787f6541b81bf63a19bbcc1742a8f7e53db0f3df3b8`.
Each report showed 508 member rows, 508 hash-field rows, 508 range-field rows,
and zero planned rows.

The repeated raw membership probes showed 508 distinct keys in `repo`, `name`,
and `oid`. The three distinct key sets were equal in each probe.

The earlier raw probe showed 509 physical rows in `name` and `oid`, but 508
distinct ranges. One old dual-encoding row causes that difference. The repair
does not copy encoding residue into `repo`; it restores the 508 logical member
identities.

CAUTION: Do not run `--execute` on a primary node without a separate approved
operation.
