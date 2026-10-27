#!/usr/bin/env python3
"""One-off CI comparison of schema lifecycle and original plus sustained read controls."""
import sys
from pathlib import Path

import declaration_flow
import schema_read_controls


def main():
    declaration_flow.main()
    position = sys.argv.index("--output") + 1
    sys.argv[position] = str(Path(sys.argv[position]) / "read-controls")
    schema_read_controls.main()


if __name__ == "__main__":
    main()
