"""Точка входа Vercel Serverless для Flask."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from write_offs_dashboard import app  # noqa: E402
