"""Точка входа Vercel (zero-config Flask)."""
from __future__ import annotations

import traceback

try:
    from write_offs_dashboard import create_app

    app = create_app()
except Exception:
    from flask import Flask

    _tb = traceback.format_exc()
    app = Flask(__name__)

    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def startup_error(path: str = ""):
        return (
            "<h1>Ошибка запуска</h1>"
            f"<pre>{_tb}</pre>"
            "<p>Проверьте логи Vercel и зависимости в requirements.txt.</p>",
            500,
            {"Content-Type": "text/html; charset=utf-8"},
        )
