"""Thin re-exports used by app.py / external entrypoints."""

from brak_dashboard.dashboard import create_app, main, register_routes, run_server

__all__ = ["create_app", "main", "register_routes", "run_server"]
