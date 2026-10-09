# LastDB Mini proxy cutover

`lastdb-proxy` owns the stable public sockets. `lastdbd` owns the database.
The proxy never opens the LastDB home. Only one worker can hold the database
lock at a time.

## Start the worker and proxy

Use private socket paths for the worker. Use public paths for the proxy.

```text
lastdbd --data-dir <home> \
  --socket-path <run>/worker.sock \
  --full-socket-path <run>/worker-full.sock

lastdb-proxy serve \
  --socket <run>/folddb.sock \
  --full-socket <run>/folddb-full.sock \
  --target <run>/worker.sock \
  --target-full-socket <run>/worker-full.sock \
  --control-socket <run>/proxy-control.sock
```

The service manager must start the proxy before it starts the worker. The
worker must stop before a replacement worker opens the same home.

## Flip a ready worker

Run the safe-upgrade health checks against the replacement worker's private
socket. Then flip the proxy target:

```text
lastdb-proxy set-target \
  --control-socket <run>/proxy-control.sock \
  --target <run>/worker-new.sock \
  --target-full-socket <run>/worker-new-full.sock
```

The control request probes both target sockets before it changes the target.
New requests use the new worker. Existing requests keep their original worker
connection. If no worker answers, the proxy returns a bounded HTTP 503 with a
retry hint.

## Safe order

1. Stop new writes if the upgrade procedure requires a write fence.
2. Stop the old worker and wait for its database lock to close.
3. Start the new worker on private sockets.
4. Run the full real-data health and durability checks.
5. Flip the proxy target through its control socket.
6. Confirm public socket reads and writes.
7. Record the cutover receipt and Situations notice.

This removes the public socket refusal during the worker handoff. It does not
permit two workers to open one home. It does not remove the need for a valid
candidate, a durable backup, or the safe-upgrade receipt.
