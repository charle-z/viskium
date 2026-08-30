"""Allow ``python -m viskium`` to behave like the installed command."""

from __future__ import annotations

from viskium.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
