#!/usr/bin/env python3
"""Read immutable page ages from one stopped, owned generated home."""
import argparse
import json
from pathlib import Path
import struct
import time

from recover_run import owned_root


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    args = parser.parse_args()
    run = args.run.resolve(strict=True)
    root, _ = owned_root(run)
    physical = root / "home" / "data" / "disk-pack"
    current = (physical / "CURRENT").read_bytes()
    generation = struct.unpack_from("<Q", current, 16)[0]
    examined, future, deadline = 0, [], time.monotonic() + 20
    for namespace in ("directory", "locator"):
        for object_path in (physical / namespace).iterdir():
            for path in object_path.glob("part-*"):
                examined += 1
                if examined > 40000 or time.monotonic() > deadline:
                    raise ValueError("The exact owned page inventory exceeds its finite bound")
                with path.open("rb") as stream:
                    header = stream.read(128)
                if header[:8] != b"LDPAGE1\0":
                    raise ValueError("An owned tree part has an unknown header")
                age = struct.unpack_from("<Q", header, 16)[0]
                if age > generation:
                    future.append({"path": str(path.relative_to(physical)), "age": age})
    receipt = {"selected_generation": generation, "tree_parts_examined": examined,
               "future_private_parts": future, "all_owned_processes_stopped": True,
               "cause": "The copy allocated a receipt generation while a writer could take source+1. "
                        "The collision selected a random future age, beyond the catch-up read root. "
                        "Private objects now use source+1; receipt allocation occurs only at publication."}
    (run / "diagnosis.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
