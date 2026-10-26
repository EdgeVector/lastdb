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


def markers(node, molecule):
    cursor, deleted, pages = None, 0, []
    for _ in range(2048):
        body = {"molecule": molecule, "cleanup_delete_markers": True,
                "after_key": cursor, "limit": 128}
        preview = node.call("/api/db/molecule-drain", body)[0]
        if preview["blocked"] or preview["deleted"] != 0:
            raise RuntimeError("Generated marker preview is blocked or changed records")
        page = node.call("/api/db/molecule-drain", {**body, "execute": True})[0]
        if (page["blocked"] or page["deleted"] != preview["eligible"]
                or page["next_cursor"] != preview["next_cursor"]):
            raise RuntimeError("Marker execute differs from its stable preview")
        pages.append(page)
        deleted += page["deleted"]
        if page["complete"]:
            again = node.call("/api/db/molecule-drain", {**body, "after_key": None})[0]
            if not again["complete"] or again["eligible"] != 0:
                raise RuntimeError("A repeat marker pass found leftover generated markers")
            return {"molecule": molecule, "deleted": deleted, "pages": pages, "repeat": again}
        next_cursor = page["next_cursor"]
        if not next_cursor or next_cursor == cursor:
            raise RuntimeError("Marker cursor did not advance")
        cursor = next_cursor
    raise RuntimeError("Marker cleanup exceeded its generated-home page bound")


def maintain(node, output, live, old, expected_live):
    # Stop the sweep before marker enumeration; no concurrent erase moves behind its cursor.
    node.stop()
    save(output / "post-measurement-shutdown.json", json.loads((output / "shutdown.json").read_text()))
    node.environment["LASTDB_TTL_SWEEP"] = "0"
    node.start()
    ages = backfill(node, live, expected_live, output)
    definition = node.call(f"/api/schema/{old}")[0]["schema"]
    molecules = definition["field_molecule_uuids"]
    if not molecules or len(molecules) != 4:
        raise RuntimeError("The generated schema lacks its four persisted molecule identities")
    cleanup = [markers(node, molecule) for molecule in sorted(set(molecules.values()))]
    if sum(row["deleted"] for row in cleanup) <= 0:
        raise RuntimeError("No real TTL delete markers reached the cleanup command")
    if node.disk_format() == 2:
        # A v2 home has no explicit compaction; its run merges are automatic.
        compact = {"format": 2, "automatic_merges": True}
    else:
        compact = node.call("/api/db/compact", {"dry_run": False, "full_copy": True})[0]
        if compact["compact"]["native_disk"]["executed"] is not True:
            raise RuntimeError("Post-cleanup compaction did not execute")
    node.stop()
    save(output / "post-cleanup-shutdown.json", json.loads((output / "shutdown.json").read_text()))
    node.start()
    save(output / "owner-maintenance.json", {"backfill": ages, "marker_cleanup": cleanup,
                                             "compaction": compact, "restarted": True,
                                             "limit": "No damaged ages or active generations were injected"})
