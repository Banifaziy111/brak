"""
Vercel entrypoint: app = Flask(__name__)
https://vercel.com/docs/frameworks/backend/flask#exporting-the-flask-application
"""
from __future__ import annotations

import traceback

from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err: Exception):
    return jsonify(
        {"error": str(err), "type": type(err).__name__}
    ), 500


def _register_dashboard() -> None:
    from write_offs_dashboard import register_routes

    register_routes(app)


try:
    _register_dashboard()
except Exception:
    _boot_trace = traceback.format_exc()

    @app.route("/")
    def boot_error():
        return (
            "<h1>Ошибка загрузки дашборда</h1>"
            f"<pre>{_boot_trace}</pre>"
            "<p>Проверьте Runtime Logs в Vercel.</p>",
            500,
            {"Content-Type": "text/html; charset=utf-8"},
        )
