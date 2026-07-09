#!/usr/bin/env python3
"""
Compatibility entrypoint for the brak write-offs dashboard.

Prefer:
  python -m brak_dashboard
or:
  python write_offs_dashboard.py
"""

from __future__ import annotations

from brak_dashboard.dashboard import (  # noqa: F401
    CACHE_TTL_SEC,
    CONFIG_PATH,
    WEEKS_CACHE_TTL_SEC,
    create_app,
    main,
    register_routes,
    run_server,
)

if __name__ == "__main__":
    raise SystemExit(main())
