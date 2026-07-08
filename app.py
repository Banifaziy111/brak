"""
Vercel entrypoint: app = Flask(__name__)
https://vercel.com/docs/frameworks/backend/flask#exporting-the-flask-application
"""
from __future__ import annotations

import traceback

from flask import Flask, jsonify
from werkzeug.exceptions import HTTPException


def _register_dashboard(application: Flask) -> None:
    from write_offs_dashboard import register_routes

    register_routes(application)


def create_app() -> Flask:
    application = Flask(__name__)

    @application.errorhandler(Exception)
    def handle_exception(err: Exception):
        if isinstance(err, HTTPException):
            return err
        return jsonify({"error": str(err), "type": type(err).__name__}), 500

    try:
        _register_dashboard(application)
    except Exception:
        boot_trace = traceback.format_exc()
        application.config["BOOT_TRACE"] = boot_trace

        @application.route("/")
        def boot_error():
            return (
                "<h1>Ошибка загрузки дашборда</h1>"
                f"<pre>{boot_trace}</pre>"
                "<p>Проверьте Runtime Logs в Vercel.</p>",
                500,
                {"Content-Type": "text/html; charset=utf-8"},
            )

        @application.route("/health")
        def boot_health():
            return jsonify({"status": "error", "detail": "dashboard boot failed"}), 500

    return application


app = create_app()
