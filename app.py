"""
Точка входа Vercel — как в документации:
https://vercel.com/docs/frameworks/backend/flask#exporting-the-flask-application
"""
from flask import Flask

from write_offs_dashboard import register_routes

app = Flask(__name__)
register_routes(app)
