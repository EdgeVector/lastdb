# Cloud purge and resync

CAUTION: This operation deletes the account's cloud content. The selected device supplies the replacement snapshot.

Use this operation when the local data must replace the cloud data. A personal account always permits the operation. An org permits its current owner or admin to authorize it.

The operation keeps account identity, devices, encryption keys, access grants, settings, and billing metadata. It deletes owned cloud files, old file versions, backup roots, share roots, message content, and usage credit records. It keeps content that another account owns.

The cloud service writes a permanent control file outside the content that it deletes. The control file identifies the account, operation, and generation. Every cloud write needs the current generation. A stale device cannot publish its old queue.

## Start the operation

The owner command uses the local node socket. The command returns a durable job ID before the snapshot work starts.

```sh
lastdb cloud purge-resync begin
lastdb cloud purge-resync begin --org <org-id>
```

Use `--request-id <uuid>` to repeat a request after a lost response. Keep the same UUID for that request.

The selected device retains an exact independent source before the cloud hold. It fetches missing file bytes before the hold. It pauses local mutations until the replacement reaches Ready and the local device acknowledges adoption.

The service preserves external file references through independent consumer copies before deletion. Then it deletes all owned cloud content. Only after deletion completes does the selected device upload the replacement snapshot.

## Read or resume a job

```sh
lastdb cloud purge-resync status --job <job-id> --json
lastdb cloud purge-resync resume --job <job-id>
```

The job keeps the exact source after a disconnect or node restart. Resume uses that source and the same operation. A large upload renews its links and resumes from completed parts.

A failure after deletion keeps the cloud hold. It also keeps the source. It cannot resume ordinary cloud sync with old data.

## Cancel before deletion

```sh
lastdb cloud purge-resync cancel --job <job-id>
```

The service permits cancellation only before deletion. It retains the new control generation and proves that the cloud content stays unchanged. Each device retains its local data before it acknowledges continuation. A device that missed an earlier Ready snapshot first adopts that snapshot.

## Other devices

Other devices hold cloud sync when they observe the control file. They retain their old local data and queues outside cloud sync. They automatically adopt the exact replacement through a clean stopped-node switch. They preserve unrelated orgs and inbound subscriptions. Old queues cannot publish under the new generation.

## Service readiness

The service refuses the purge until it proves the old upload cutoff, exact account content, and external references. All service functions and rollback versions must enforce the generation protocol. The service retains the normal deployment and safe upgrade gates.

A build result does not prove a cloud purge. Use the real command and deployed service on an independent owned account. Keep the primary account intact during product proof.

```sh
lastdb --data-dir <owned-account-home> cloud purge-resync status \
  --job <job-id> --proof-receipt <receipt-path> --json
```

The receipt requires Ready and the completed local generation switch. It binds the exact account, operation, generation, and replacement snapshot.
