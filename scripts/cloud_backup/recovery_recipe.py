"""An explicit, durable recipe for the generated CI data and recovery roles."""

import hashlib

from ci_control import decimal, digest
from support import canonical_uuid, require

ROLES = ("base_snapshot", "mutation_log", "final_snapshot")


def file_digest(source):
    payload = hashlib.shake_256(("Cloud automatic CI " + source).encode()).digest(65536)
    return hashlib.sha256(payload).hexdigest()


def validate(recipe, verified, source):
    require(isinstance(recipe, dict) and set(recipe) == {
        "schema", "name", *ROLES, "live_file_bytes", "live_file_sha256"}
        and recipe["schema"] == "lastdb.cloud-ci-recovery-recipe.v1"
        and recipe["name"] == "automatic-log-snapshot-64k-v1"
        and decimal(recipe["live_file_bytes"]) == 65536
        and digest(recipe["live_file_sha256"]) == file_digest(source), "ci_recovery_recipe")
    for role in ROLES:
        point = recipe[role]
        require(isinstance(point, dict) and set(point) == {"stream_id", "commit_id", "commit_sha256"}
                and point in verified, "ci_recovery_recipe_verified_role")
        canonical_uuid(point["stream_id"])
        require(canonical_uuid(point["commit_id"]).startswith("a17dbac2-"), "ci_recipe_commit")
        digest(point["commit_sha256"])
    require(len({recipe[role]["commit_id"] for role in ROLES}) == 3, "ci_recovery_recipe_distinct")
    return recipe


def create(points, source, expected):
    require(len(points) == 3 and expected["file_bytes"] == 65536
            and expected["file_sha256"] == file_digest(source), "ci_recipe_actual_file")
    value = {"schema": "lastdb.cloud-ci-recovery-recipe.v1",
             "name": "automatic-log-snapshot-64k-v1", **dict(zip(ROLES, points)),
             "live_file_bytes": "65536", "live_file_sha256": expected["file_sha256"]}
    return validate(value, points, source)
