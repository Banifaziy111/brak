#!/usr/bin/env python3
"""
Просмотр brak_team.write_offs в браузере (HTML) или выгрузка в .html файл.

Настройка:
  1. Скопируйте .env.example в .env и укажите пароль
  2. pip install -r requirements.txt

Запуск веб-интерфейса:
  python write_offs_html.py

Статический отчёт (первые N строк по фильтру):
  python write_offs_html.py --export report.html --limit 5000
  python write_offs_html.py --export report.html --office-id 120762 --wh-id 133
"""

from __future__ import annotations

import argparse
import html
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

COLUMNS = (
    ("shk_id", "ШК"),
    ("date", "Дата"),
    ("type", "Тип"),
    ("total_cost", "Стоимость"),
    ("amount", "Сумма"),
    ("share", "Доля"),
    ("office_id", "Офис"),
    ("wh_id", "Блок"),
    ("nm_id", "nm_id"),
    ("subject_name", "Предмет"),
    ("parent_name", "Родитель"),
    ("title", "Наименование"),
    ("brand_name", "Бренд"),
    ("state_id", "state_id"),
    ("reason_id", "reason_id"),
    ("reason_descr", "Причина"),
    ("seller_id", "seller_id"),
    ("supplier_id", "supplier_id"),
    ("owner_product", "Владелец"),
    ("cnt_org", "cnt_org"),
    ("cnt_ors", "cnt_ors"),
    ("cnt_ocr", "cnt_ocr"),
)

COL_NAMES = [c[0] for c in COLUMNS]


def db_config() -> dict[str, Any]:
    return {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME", "botdb"),
        "user": os.environ.get("DB_USER", ""),
        "password": os.environ.get("DB_PASSWORD", ""),
        "connect_timeout": 15,
    }


@contextmanager
def get_conn():
    import psycopg2

    conn = psycopg2.connect(**db_config())
    try:
        yield conn
    finally:
        conn.close()


def build_where(args: argparse.Namespace) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if args.office_id is not None:
        clauses.append("office_id = %s")
        params.append(args.office_id)
    if args.wh_id is not None:
        clauses.append("wh_id = %s")
        params.append(args.wh_id)
    if args.type:
        clauses.append("type = %s")
        params.append(args.type)
    if args.date_from:
        clauses.append("date >= %s")
        params.append(args.date_from)
    if args.date_to:
        clauses.append("date < %s::date + interval '1 day'")
        params.append(args.date_to)
    if args.search:
        clauses.append("(title ILIKE %s OR reason_descr ILIKE %s OR brand_name ILIKE %s)")
        q = f"%{args.search}%"
        params.extend([q, q, q])

    if not clauses:
        return "", params
    return " WHERE " + " AND ".join(clauses), params


def fetch_page(
    where_sql: str,
    params: list[Any],
    *,
    limit: int,
    offset: int,
) -> tuple[list[tuple], int]:
    cols = ", ".join(COL_NAMES)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM brak_team.write_offs{where_sql}",
            params,
        )
        total = int(cur.fetchone()[0])
        cur.execute(
            f"""
            SELECT {cols}
            FROM brak_team.write_offs
            {where_sql}
            ORDER BY date DESC NULLS LAST, shk_id
            LIMIT %s OFFSET %s
            """,
            [*params, limit, offset],
        )
        rows = cur.fetchall()
    return rows, total


def format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def rows_to_html_table(rows: list[tuple]) -> str:
    headers = "".join(f"<th>{html.escape(label)}</th>" for _, label in COLUMNS)
    body_parts: list[str] = []
    for row in rows:
        cells = "".join(
            f'<td title="{html.escape(format_cell(v))}">{html.escape(format_cell(v))}</td>'
            for v in row
        )
        body_parts.append(f"<tr>{cells}</tr>")
    return f"""
<table>
  <thead><tr>{headers}</tr></thead>
  <tbody>{"".join(body_parts)}</tbody>
</table>
"""


PAGE_CSS = """
:root { font-family: system-ui, Segoe UI, sans-serif; font-size: 14px; }
body { margin: 0; padding: 16px; background: #f4f6f8; color: #1a1a1a; }
h1 { margin: 0 0 8px; font-size: 1.25rem; }
.meta { color: #555; margin-bottom: 16px; }
.filters { background: #fff; padding: 12px 16px; border-radius: 8px;
           box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 16px;
           display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }
.filters label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: #444; }
.filters input, .filters select { padding: 6px 8px; border: 1px solid #ccc; border-radius: 4px; }
.filters button { padding: 8px 16px; background: #2563eb; color: #fff; border: none;
                  border-radius: 4px; cursor: pointer; }
.filters button:hover { background: #1d4ed8; }
.table-wrap { overflow: auto; background: #fff; border-radius: 8px;
              box-shadow: 0 1px 3px rgba(0,0,0,.08); max-height: calc(100vh - 220px); }
table { border-collapse: collapse; width: max-content; min-width: 100%; }
th, td { border-bottom: 1px solid #e5e7eb; padding: 6px 10px; text-align: left;
          max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
th { position: sticky; top: 0; background: #f9fafb; z-index: 1; font-weight: 600; }
tr:hover td { background: #f0f7ff; }
.pager { margin-top: 12px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.pager a, .pager span { padding: 6px 12px; background: #fff; border-radius: 4px;
                        text-decoration: none; color: #2563eb; border: 1px solid #ddd; }
.pager span.current { background: #2563eb; color: #fff; border-color: #2563eb; }
.pager a:hover { background: #eff6ff; }
"""


def full_html_document(title: str, body_inner: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>{PAGE_CSS}</style>
</head>
<body>
{body_inner}
</body>
</html>
"""


def export_static_html(path: Path, args: argparse.Namespace) -> None:
    where_sql, params = build_where(args)
    rows, total = fetch_page(where_sql, params, limit=args.limit, offset=0)
    body = f"""
<h1>brak_team.write_offs</h1>
<p class="meta">Показано {len(rows):,} из {total:,} строк (лимит export: {args.limit:,})</p>
<div class="table-wrap">
{rows_to_html_table(rows)}
</div>
"""
    path.write_text(full_html_document("write_offs", body), encoding="utf-8")
    print(f"Сохранено: {path} ({len(rows):,} строк)")


def run_server(args: argparse.Namespace) -> None:
    from flask import Flask, request
    from urllib.parse import urlencode

    app = Flask(__name__)

    def ns_from_request() -> argparse.Namespace:
        return argparse.Namespace(
            office_id=request.args.get("office_id", type=int),
            wh_id=request.args.get("wh_id", type=int),
            type=request.args.get("type") or None,
            date_from=request.args.get("date_from") or None,
            date_to=request.args.get("date_to") or None,
            search=request.args.get("search") or None,
        )

    @app.route("/")
    def index():
        page = max(1, request.args.get("page", 1, type=int))
        per_page = min(500, max(10, request.args.get("per_page", 100, type=int)))
        flt = ns_from_request()
        where_sql, params = build_where(flt)
        rows, total = fetch_page(
            where_sql, params, limit=per_page, offset=(page - 1) * per_page
        )
        pages = max(1, (total + per_page - 1) // per_page)

        def page_url(p: int) -> str:
            q = {k: v for k, v in request.args.items() if k not in ("page",)}
            q["page"] = str(p)
            return "?" + urlencode(q)

        pager = []
        if page > 1:
            pager.append(f'<a href="{page_url(page - 1)}">← Назад</a>')
        pager.append(f'<span class="current">стр. {page} / {pages}</span>')
        if page < pages:
            pager.append(f'<a href="{page_url(page + 1)}">Вперёд →</a>')

        filters = f"""
<form class="filters" method="get">
  <label>Офис <input name="office_id" type="number" value="{html.escape(request.args.get('office_id') or '')}"></label>
  <label>Блок <input name="wh_id" type="number" value="{html.escape(request.args.get('wh_id') or '')}"></label>
  <label>Тип <input name="type" value="{html.escape(request.args.get('type') or '')}"></label>
  <label>Дата от <input name="date_from" type="date" value="{html.escape(request.args.get('date_from') or '')}"></label>
  <label>Дата до <input name="date_to" type="date" value="{html.escape(request.args.get('date_to') or '')}"></label>
  <label>Поиск <input name="search" placeholder="title, причина, бренд" value="{html.escape(request.args.get('search') or '')}"></label>
  <label>На стр. <input name="per_page" type="number" min="10" max="500" value="{per_page}"></label>
  <button type="submit">Применить</button>
</form>
"""

        body = f"""
<h1>brak_team.write_offs</h1>
<p class="meta">Всего по фильтру: <b>{total:,}</b> · показано {len(rows):,} на странице</p>
{filters}
<div class="table-wrap">{rows_to_html_table(rows)}</div>
<div class="pager">{"".join(pager)}</div>
"""
        return full_html_document("write_offs", body)

    host = os.environ.get("HTML_HOST", "127.0.0.1")
    port = int(os.environ.get("HTML_PORT", "8080"))
    print(f"Откройте в браузере: http://{host}:{port}/")
    app.run(host=host, port=port, debug=False)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="HTML просмотр brak_team.write_offs")
    p.add_argument("--export", type=Path, metavar="FILE.html", help="Сохранить статический HTML")
    p.add_argument("--limit", type=int, default=5000, help="Строк при --export (макс. для файла)")
    p.add_argument("--office-id", type=int)
    p.add_argument("--wh-id", type=int)
    p.add_argument("--type", help="Фильтр type")
    p.add_argument("--date-from")
    p.add_argument("--date-to")
    p.add_argument("--search", help="Поиск в title / reason_descr / brand_name")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not os.environ.get("DB_PASSWORD"):
        print("Создайте файл .env (см. .env.example) с DB_PASSWORD", file=sys.stderr)
        return 1

    try:
        if args.export:
            export_static_html(args.export.resolve(), args)
        else:
            run_server(args)
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
