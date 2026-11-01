"""Run the requested owner maintenance on the already measured generated home."""

import json

from node import save


def backfill(node, schema, expected, output):
    cursor, tracked, pages = None, 0, []
    for _ in range(2048):
        page = node.call("/api/db/schema-retention", {
            "action": "backfill", "schema": schema, "confirm_expire_existing": True,
            "limit": 64, "cursor": cursor})[0]
        pages.append(page)
        tracked += page["tracked"]
        save(output / "backfill-pages.json", {"schema": schema, "expected": expected,
                                               "tracked": tracked, "pages": pages})
        if page["complete"]:
            if tracked != expected:
                raise RuntimeError(f"Backfill tracked {tracked} keys; expected {expected}")
            return pages
        next_cursor = page["next_cursor"]
        if not next_cursor or next_cursor == cursor:
            raise RuntimeError("Backfill cursor did not advance")
        cursor = next_cursor
    raise RuntimeError("Backfill exceeded the generated-home page bound")


def maintain(node, output, live, expected_live):
    # Stop the sweep before the backfill; no concurrent erase moves behind its cursor.
    # The delete-marker cleanup page left the API with the older tools, so
    # this step no longer runs it.
    node.stop()
    save(output / "post-measurement-shutdown.json", json.loads((output / "shutdown.json").read_text()))
    node.environment["LASTDB_TTL_SWEEP"] = "0"
    node.start()
    ages = backfill(node, live, expected_live, output)
    # A v2 home has no explicit compaction; its run merges are automatic.
    compact = {"format": 2, "automatic_merges": True}
    node.stop()
    save(output / "post-cleanup-shutdown.json", json.loads((output / "shutdown.json").read_text()))
    node.start()
    save(output / "owner-maintenance.json", {"backfill": ages,
                                             "compaction": compact, "restarted": True,
                                             "limit": "No damaged ages or active generations were injected"})
