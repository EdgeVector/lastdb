"""Step 17 of the personal database proof: a relay answer that says try again is tried again.

The reference relay answers the first `log/streams` request after each start with HTTP 429
`rate_limited` and the field `provider.dynamo.conflict`, which is what the Exemem relay says when a
transaction of the request collided with another. A client signs a request with a nonce that the
relay consumes even when it refuses the request, so a retry that reused the nonce would get 401.
The reference relay refuses a nonce it has seen when it is run for this step.

    daemon:   a sync turn meets the 429, sends the request again, and the turn does not fail.
    restore:  `lastdb backup restore` meets the 429 and still restores every entry.
    quota:    the same 429 with the field `quota.bytes` is final: one request, one failure that
              carries the relay's message, and no home.
"""
import subprocess

from personal_proof_nodes import ALPHA, FIELDS, create, require, wait
from record_runs import definition, system_seed

COLLISION = "provider.dynamo.conflict"
QUOTA = "quota.bytes"


def restart(relay, field):
    """Restart the relay so that its first `log/streams` request is refused with `field`. Returns the log mark."""
    relay.stop()
    relay.refuse_first = [f"log/streams={field}"]
    relay.start()
    return len(relay.lines())


def streams_since(relay, mark):
    return [status for status, _entries in relay.requests_since(mark, "log/streams")]


def prove_retries(relay, m, n, p):
    """M is a running daemon, N a restore, P a restore that meets a quota refusal. All use `relay`."""
    relay.start()
    phrase = m.init()
    m.start()
    system_seed(m.node, definition(ALPHA, "Hash", FIELDS, description_prefix="Personal proof "))
    m.write([create(ALPHA, "r1", "one")])
    wait("M to upload", m.synced_up)
    m.stop()
    clean = len(m.warnings(all_lines=True))

    # The daemon: its first turn meets the 429.
    mark = restart(relay, COLLISION)
    m.start()
    wait("the relay to refuse a log/streams request and then serve it",
         lambda: streams_since(relay, mark)[:2] == ["429", "200"])
    m.write([create(ALPHA, "r2", "two")])
    wait("M to upload after the refusal", m.synced_up)
    require(len(m.warnings(all_lines=True)) == clean, f"a sync turn of M failed after the 429: {m.warnings()!r}")
    written = m.spool_state()["head"]

    # The restore.
    mark = restart(relay, COLLISION)
    restored = n.restore(phrase)
    require(streams_since(relay, mark)[:2] == ["429", "200"], f"N's restore: {streams_since(relay, mark)!r}")
    require(restored["replayed"] == written, f"N restore: {restored!r}, expected {written} entries")

    # A quota refusal is final.
    mark = restart(relay, QUOTA)
    result = subprocess.run([str(p.cli), *p.restore_arguments()], input=phrase, capture_output=True, text=True,
                            timeout=300, check=False, env=p.cli_env())
    require(result.returncode != 0 and "rate_limited" in result.stderr and QUOTA in result.stderr,
            f"the restore did not fail with the quota refusal: {result.returncode} {result.stderr[-500:]!r}")
    require(streams_since(relay, mark) == ["429"], f"the quota refusal was tried again: {streams_since(relay, mark)!r}")
    require(not p.node.home.exists(), "a refused restore left a home")
    print(f"17. a 429 that says try again ({COLLISION}) is tried again with a new signature: a running daemon and a "
          f"restore of {written} entries succeed; the quota refusal ({QUOTA}) fails at once, with one request", flush=True)
    m.stop()
    return {"collision_field": COLLISION, "quota_field": QUOTA, "restored_entries": written}
