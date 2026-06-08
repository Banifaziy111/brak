#!/usr/bin/env python3
"""
Дашборд brak_team.write_offs — 4 таблицы ТОП-20 как в отчёте.

  python write_offs_dashboard.py
  → http://127.0.0.1:8080/

Фильтр: все WH, корпус (несколько wh_id из wh_buildings.json) или свой набор галочками.
Настройка корпусов: wh_buildings.json
"""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "wh_buildings.json"

_env_file = ROOT / ".env"
if _env_file.exists():
    load_dotenv(_env_file)


@dataclass
class Row:
    row_id: int | None
    name: str
    amounts: dict[int, float]

    def amount(self, week: int) -> float:
        return self.amounts.get(week, 0.0)

    def dynamics(self, week_prev: int, week_last: int) -> float | None:
        w_prev, w_last = self.amount(week_prev), self.amount(week_last)
        if w_prev == 0:
            return None if w_last == 0 else 100.0
        return (w_last - w_prev) / w_prev * 100

    def average(self, week_prev: int, week_last: int) -> float:
        return (self.amount(week_prev) + self.amount(week_last)) / 2

    def pct_vs_avg(self, week_prev: int, week_last: int) -> float | None:
        avg = self.average(week_prev, week_last)
        if avg == 0:
            return None
        return self.amount(week_last) / avg * 100


def normalize_database_url(url: str) -> str:
    """postgresql+psycopg://… → postgresql://… для psycopg2."""
    from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

    url = url.strip()
    if "://" in url:
        scheme, rest = url.split("://", 1)
        if "+" in scheme:
            scheme = scheme.split("+", 1)[0]
        url = f"{scheme}://{rest}"

    parsed = urlparse(url)
    if not parsed.scheme:
        return url

    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k and v]
    if not query and parsed.query:
        # обрезанный ?sslmode без значения
        pass
    fixed = parsed._replace(query=urlencode(query))
    return urlunparse(fixed)


def db_config() -> dict[str, Any]:
    timeout = int(os.environ.get("DB_CONNECT_TIMEOUT", "15"))
    host = os.environ.get("DB_HOST", "").strip()
    user = os.environ.get("DB_USER", "").strip()
    password = os.environ.get("DB_PASSWORD", "")

    # Локально: DB_* надёжнее, если DATABASE_URL в формате SQLAlchemy
    if host and user and password:
        cfg: dict[str, Any] = {
            "host": host,
            "port": int(os.environ.get("DB_PORT", "5432")),
            "dbname": os.environ.get("DB_NAME", "botdb"),
            "user": user,
            "password": password,
            "connect_timeout": timeout,
        }
        sslmode = os.environ.get("DB_SSLMODE")
        if sslmode:
            cfg["sslmode"] = sslmode
        return cfg

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if database_url:
        return {"dsn": normalize_database_url(database_url), "connect_timeout": timeout}

    return {
        "host": host or "localhost",
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME", "botdb"),
        "user": user,
        "password": password,
        "connect_timeout": timeout,
    }


def check_db_env() -> str | None:
    if os.environ.get("DATABASE_URL", "").strip():
        return None
    missing = [k for k in ("DB_HOST", "DB_USER", "DB_PASSWORD") if not os.environ.get(k)]
    if missing:
        return "Не заданы: DATABASE_URL или " + ", ".join(missing)
    return None


@contextmanager
def get_conn():
    import psycopg2

    conn = psycopg2.connect(**db_config())
    try:
        yield conn
    finally:
        conn.close()


def load_config() -> dict:
    if CONFIG_PATH.exists():
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    return {
        "office_id": None,
        "week_year": 2026,
        "week_prev": 20,
        "week_last": 21,
        "wh_catalog": [],
        "buildings": [{"id": "all", "name": "Все WH", "wh_ids": []}],
    }


def catalog_wh_ids(cfg: dict) -> list[int]:
    catalog = cfg.get("wh_catalog") or []
    return [int(w["wh_id"]) for w in catalog]


def to_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def fetch_wh_list(office_id: int | None) -> list[dict]:
    clauses = []
    params: list[Any] = []
    if office_id is not None:
        clauses.append("office_id = %s")
        params.append(office_id)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT wh_id,
               COUNT(*) AS cnt,
               ROUND(SUM(amount)::numeric, 0) AS total_amount
        FROM brak_team.write_offs
        {where}
        GROUP BY wh_id
        ORDER BY wh_id
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [
        {"wh_id": r[0], "cnt": r[1], "total_amount": to_float(r[2])}
        for r in rows
    ]


def fetch_db_stats(
    office_id: int | None,
    wh_ids: list[int] | None,
) -> dict[str, Any]:
    clauses: list[str] = []
    params: list[Any] = []
    if office_id is not None:
        clauses.append("office_id = %s")
        params.append(office_id)
    if wh_ids:
        clauses.append("wh_id = ANY(%s)")
        params.append(wh_ids)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT COUNT(*)::bigint,
               MAX(date)::text,
               COALESCE(ROUND(SUM(amount)::numeric, 0), 0)
        FROM brak_team.write_offs
        {where}
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
    return {
        "row_count": int(row[0]) if row else 0,
        "max_date": row[1] if row else None,
        "total_amount": to_float(row[2]) if row else 0.0,
    }


def run_db_refresh() -> str | None:
    """
    Опционально запускает обновление источника (ETL) перед чтением отчёта.
    DB_REFRESH_SQL — SQL на сервере (функция/REFRESH).
    DB_REFRESH_URL — HTTP-вызов внешней выгрузки.
  """
    import urllib.error
    import urllib.request

    sql = os.environ.get("DB_REFRESH_SQL", "").strip()
    if sql:
        with get_conn() as conn:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql)
            try:
                row = cur.fetchone()
                if row and row[0] is not None:
                    return str(row[0])
            except Exception:
                pass
        return "SQL обновления выполнен"

    url = os.environ.get("DB_REFRESH_URL", "").strip()
    if not url:
        return None

    method = os.environ.get("DB_REFRESH_METHOD", "POST").upper()
    req = urllib.request.Request(url, method=method)
    token = os.environ.get("DB_REFRESH_TOKEN", "").strip()
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    timeout = int(os.environ.get("DB_REFRESH_TIMEOUT", "120"))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")[:300]
            return body or f"HTTP {resp.status}"
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"DB_REFRESH_URL: HTTP {exc.code} {detail}") from exc


def fetch_available_weeks(
    year: int,
    office_id: int | None,
    wh_ids: list[int] | None,
) -> list[int]:
    clauses: list[str] = ["date IS NOT NULL", "EXTRACT(ISOYEAR FROM date) = %s"]
    params: list[Any] = [year]
    if office_id is not None:
        clauses.append("office_id = %s")
        params.append(office_id)
    if wh_ids:
        clauses.append("wh_id = ANY(%s)")
        params.append(wh_ids)
    where = " WHERE " + " AND ".join(clauses)
    sql = f"""
        SELECT DISTINCT EXTRACT(WEEK FROM date)::int AS w
        FROM brak_team.write_offs
        {where}
        ORDER BY w
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [int(r[0]) for r in rows]


def fetch_top(
    *,
    group_col: str,
    id_col: str | None,
    wh_ids: list[int] | None,
    office_id: int | None,
    org0_only: bool,
    year: int,
    weeks: list[int],
    week_last: int,
    limit: int = 20,
) -> list[Row]:
    if group_col not in ("reason_descr", "parent_name"):
        raise ValueError("invalid group_col")

    clauses: list[str] = []
    params: list[Any] = []

    if office_id is not None:
        clauses.append("office_id = %s")
        params.append(office_id)
    if wh_ids:
        clauses.append("wh_id = ANY(%s)")
        params.append(wh_ids)
    if org0_only:
        clauses.append("cnt_org = 0")

    clauses.append("date IS NOT NULL")
    clauses.append("EXTRACT(ISOYEAR FROM date) = %s")
    params.append(year)

    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    if not weeks:
        weeks = [week_last]

    week_cols = ",\n            ".join(
        f"COALESCE(SUM(amount) FILTER (WHERE EXTRACT(WEEK FROM date) = %s), 0) AS w_{w}"
        for w in weeks
    )

    if id_col:
        # Один ИД — одна строка (в БД reason_descr может отличаться при том же reason_id)
        sql = f"""
        SELECT
            {id_col} AS row_id,
            COALESCE(MAX({group_col}), '—') AS name,
            {week_cols}
        FROM brak_team.write_offs
        {where}
        GROUP BY {id_col}
        ORDER BY w_{week_last} DESC NULLS LAST
        LIMIT %s
    """
    else:
        sql = f"""
        SELECT
            NULL::int AS row_id,
            COALESCE({group_col}, '—') AS name,
            {week_cols}
        FROM brak_team.write_offs
        {where}
        GROUP BY {group_col}
        ORDER BY w_{week_last} DESC NULLS LAST
        LIMIT %s
    """
    qparams = [*weeks, *params, limit]

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, qparams)
        raw = cur.fetchall()

    out: list[Row] = []
    for r in raw:
        amounts = {w: to_float(r[2 + i]) for i, w in enumerate(weeks)}
        out.append(
            Row(
                row_id=int(r[0]) if r[0] is not None else None,
                name=str(r[1]),
                amounts=amounts,
            )
        )
    return out


def add_shares(rows: list[Row], week_prev: int, week_last: int) -> list[dict]:
    total_last = sum(r.amount(week_last) for r in rows)
    out: list[dict] = []
    for i, r in enumerate(rows, start=1):
        share = (r.amount(week_last) / total_last * 100) if total_last else 0
        out.append(
            {
                "num": i,
                "row_id": r.row_id,
                "name": r.name,
                "amounts": dict(r.amounts),
                "w_prev": r.amount(week_prev),
                "w_last": r.amount(week_last),
                "dynamics": r.dynamics(week_prev, week_last),
                "share": share,
                "average": r.average(week_prev, week_last),
                "pct_vs_avg": r.pct_vs_avg(week_prev, week_last),
            }
        )
    return out


def totals(rows: list[dict], weeks: list[int], week_prev: int, week_last: int) -> dict:
    amounts = {w: sum(x["amounts"].get(w, 0) for x in rows) for w in weeks}
    w_prev = amounts.get(week_prev, 0)
    w_last = amounts.get(week_last, 0)
    avg = (w_prev + w_last) / 2 if rows else 0
    dyn = ((w_last - w_prev) / w_prev * 100) if w_prev else None
    pct_avg = (w_last / avg * 100) if avg else None
    return {
        "amounts": amounts,
        "w_prev": w_prev,
        "w_last": w_last,
        "dynamics": dyn,
        "share": 100.0,
        "average": avg,
        "pct_vs_avg": pct_avg,
    }


def fmt_num(n: float) -> str:
    return f"{n:,.0f}".replace(",", " ")


def fmt_pct(n: float | None) -> str:
    if n is None:
        return "—"
    return f"{n:.2f}%"


def heat_style(value: float | None, mode: str) -> str:
    if value is None:
        return ""
    if mode == "dynamics":
        # рост брака — краснее
        v = max(-50, min(50, value))
        if v <= 0:
            return "background:#d4edda;color:#155724" if v < -2 else "background:#fff3cd"
        return "background:#f8d7da;color:#721c24" if v > 2 else "background:#fff3cd"
    if mode == "share":
        v = max(0, min(20, value))
        alpha = v / 20
        return f"background:rgba(255,199,206,{0.15 + alpha * 0.5})"
    # pct_vs_avg
    v = value - 100
    v = max(-30, min(30, v))
    if v < -2:
        return "background:#c6efce"
    if v > 2:
        return "background:#ffc7ce"
    return "background:#ffeb9c"


def _week_cell_style(week: int, week_prev: int, week_last: int) -> str:
    if week == week_last:
        return "background:#fff2cc;font-weight:600"
    if week == week_prev:
        return "background:#e2efda"
    return ""


def render_table(
    title: str,
    rows: list[dict],
    total: dict,
    *,
    show_id: bool,
    weeks: list[int],
    week_prev: int,
    week_last: int,
    name_header: str = "Наименование",
) -> str:
    id_hdr = (
        "<th class='sticky col-id'>ИД</th>"
        if show_id
        else "<th class='sticky col-id'>№</th>"
    )
    week_hdrs = "".join(
        f"<th class='n week-col'>{w}</th>" for w in weeks
    )
    body = []
    for r in rows:
        id_cell = (
            f"<td class='c sticky col-id'>{r['row_id']}</td>"
            if show_id
            else f"<td class='c sticky col-id'>{r['num']}</td>"
        )
        week_cells = "".join(
            f"<td class='n week-col' style='{_e(_week_cell_style(w, week_prev, week_last))}'>"
            f"{fmt_num(r['amounts'].get(w, 0))}</td>"
            for w in weeks
        )
        body.append(
            f"<tr>{id_cell}"
            f"<td class='name sticky col-name' title='{_e(r['name'])}'>{_e(r['name'])}</td>"
            f"{week_cells}"
            f"<td class='n metric' style='{_e(heat_style(r['dynamics'], 'dynamics'))}'>{fmt_pct(r['dynamics'])}</td>"
            f"<td class='n metric' style='{_e(heat_style(r['share'], 'share'))}'>{fmt_pct(r['share'])}</td>"
            f"<td class='n metric'>{fmt_num(r['average'])}</td>"
            f"<td class='n metric' style='{_e(heat_style(r['pct_vs_avg'], 'pct_vs_avg'))}'>{fmt_pct(r['pct_vs_avg'])}</td>"
            "</tr>"
        )

    t = total
    week_total_cells = "".join(
        f"<td class='n week-col' style='{_e(_week_cell_style(w, week_prev, week_last))}'>"
        f"<b>{fmt_num(t['amounts'].get(w, 0))}</b></td>"
        for w in weeks
    )
    body.append(
        f"<tr class='total'>"
        f"<td colspan='2' class='sticky col-id'><b>Итого</b></td>"
        f"{week_total_cells}"
        f"<td class='n metric' style='{_e(heat_style(t['dynamics'], 'dynamics'))}'><b>{fmt_pct(t['dynamics'])}</b></td>"
        f"<td class='n metric'><b>{fmt_pct(t['share'])}</b></td>"
        f"<td class='n metric'><b>{fmt_num(t['average'])}</b></td>"
        f"<td class='n metric' style='{_e(heat_style(t['pct_vs_avg'], 'pct_vs_avg'))}'><b>{fmt_pct(t['pct_vs_avg'])}</b></td>"
        f"</tr>"
    )

    return f"""
<section class="panel">
  <h2>{_e(title)}</h2>
  <div class="table-scroll">
  <table>
    <colgroup>
      <col class="col-id">
      <col class="col-name">
    </colgroup>
    <thead>
      <tr>
        {id_hdr}
        <th class="sticky col-name">{_e(name_header)}</th>
        {week_hdrs}
        <th class="metric">Динамика {week_last} к {week_prev}</th>
        <th class="metric">Доля от общего брака</th>
        <th class="metric">Среднее за 2 нед.</th>
        <th class="metric">% посл. к средней</th>
      </tr>
    </thead>
    <tbody>{''.join(body)}</tbody>
  </table>
  </div>
</section>
"""


def _e(s: Any) -> str:
    import html

    return html.escape(str(s)) if s is not None else ""


def build_report_data(
    wh_ids: list[int] | None,
    office_id: int | None,
    year: int,
    week_prev: int,
    week_last: int,
    weeks: list[int] | None = None,
) -> dict:
    if weeks is None:
        weeks = fetch_available_weeks(year, office_id, wh_ids)
    if not weeks:
        weeks = [week_prev, week_last]
    if week_prev not in weeks:
        weeks = sorted(set(weeks) | {week_prev})
    if week_last not in weeks:
        weeks = sorted(set(weeks) | {week_last})

    defects = fetch_top(
        group_col="reason_descr",
        id_col="reason_id",
        wh_ids=wh_ids,
        office_id=office_id,
        org0_only=False,
        year=year,
        weeks=weeks,
        week_last=week_last,
    )
    defects_org0 = fetch_top(
        group_col="reason_descr",
        id_col="reason_id",
        wh_ids=wh_ids,
        office_id=office_id,
        org0_only=True,
        year=year,
        weeks=weeks,
        week_last=week_last,
    )
    cats = fetch_top(
        group_col="parent_name",
        id_col=None,
        wh_ids=wh_ids,
        office_id=office_id,
        org0_only=False,
        year=year,
        weeks=weeks,
        week_last=week_last,
    )
    cats_org0 = fetch_top(
        group_col="parent_name",
        id_col=None,
        wh_ids=wh_ids,
        office_id=office_id,
        org0_only=True,
        year=year,
        weeks=weeks,
        week_last=week_last,
    )

    d_rows = add_shares(defects, week_prev, week_last)
    d0_rows = add_shares(defects_org0, week_prev, week_last)
    c_rows = add_shares(cats, week_prev, week_last)
    c0_rows = add_shares(cats_org0, week_prev, week_last)

    return {
        "weeks": weeks,
        "defects": d_rows,
        "defects_total": totals(d_rows, weeks, week_prev, week_last),
        "defects_org0": d0_rows,
        "defects_org0_total": totals(d0_rows, weeks, week_prev, week_last),
        "categories": c_rows,
        "categories_total": totals(c_rows, weeks, week_prev, week_last),
        "categories_org0": c0_rows,
        "categories_org0_total": totals(c0_rows, weeks, week_prev, week_last),
    }


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Брак — ТОП-20</title>
<style>
* { box-sizing: border-box; }
body { margin: 0; font: 13px/1.35 Calibri, "Segoe UI", sans-serif; background: #e8eaed; color: #000; }
header { background: #1f4e79; color: #fff; padding: 12px 16px; }
header h1 { margin: 0; font-size: 18px; font-weight: 600; }
.toolbar { background: #fff; padding: 12px 16px; border-bottom: 1px solid #ccc;
           display: flex; flex-wrap: wrap; gap: 16px; align-items: flex-start; }
.group { border: 1px solid #ddd; border-radius: 6px; padding: 8px 12px; background: #fafafa; }
.group legend { font-weight: 600; font-size: 12px; padding: 0 4px; }
.building-btns { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 6px; }
.building-btns button { padding: 6px 12px; border: 1px solid #1f4e79; background: #fff;
  border-radius: 4px; cursor: pointer; font-size: 12px; }
.building-btns button.active { background: #1f4e79; color: #fff; }
.wh-grid { display: flex; flex-direction: column; gap: 2px;
           max-height: 280px; overflow: auto; margin-top: 6px; min-width: 340px; }
.wh-grid label { font-size: 11px; display: flex; align-items: center; gap: 6px; cursor: pointer; }
.wh-grid .corpus-hdr { font-weight: 700; font-size: 11px; color: #1f4e79; margin-top: 8px;
                      padding: 4px 0 2px; border-bottom: 1px solid #ccc; }
.wh-grid .corpus-hdr:first-child { margin-top: 0; }
.weeks input, .weeks select { padding: 4px; margin-right: 8px; font-size: 12px; }
.weeks select { min-width: 64px; }
.weeks .hint { display: block; font-size: 11px; color: #666; margin-top: 6px; max-width: 320px; }
.panel .table-scroll { overflow-x: auto; max-width: 100%; }
.panel table { min-width: max-content; }
th.week-col, td.week-col { min-width: 72px; }
th.metric, td.metric { min-width: 110px; }
th.metric { background: #1f4e79; color: #fff; }
td.metric { background: #f5f8fc; color: #000; }
th.sticky, td.sticky { position: sticky; z-index: 2; }
th.sticky { background: #1f4e79; z-index: 4; }
td.sticky { background: #fff; }
.col-id { width: 52px; min-width: 52px; }
.col-name { width: 260px; min-width: 200px; }
th.col-id, td.col-id { left: 0; }
th.col-name, td.col-name { left: 52px; box-shadow: 2px 0 4px rgba(0,0,0,.08); }
td.col-name { white-space: normal; line-height: 1.25; vertical-align: top; }
tr.total td.sticky { background: #d9e2f3; }
tr.total th.sticky { background: #1f4e79; }
.actions { display: flex; align-items: flex-end; gap: 8px; }
.actions button { padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer;
  font-weight: 600; font-size: 12px; }
.actions button.primary { background: #1f4e79; color: #fff; }
.actions button.primary:hover { background: #163a5c; }
.actions button.primary:disabled { opacity: 0.6; cursor: wait; }
.actions button.secondary { background: #217346; color: #fff; }
.actions button.secondary:hover { background: #1a5c38; }
#status { padding: 8px 16px; color: #555; font-size: 12px; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; padding: 12px; }
@media (max-width: 1200px) { .grid { grid-template-columns: 1fr; } }
.panel { background: #fff; border: 1px solid #999; overflow: auto; }
.panel h2 { margin: 0; padding: 8px 10px; font-size: 13px; font-weight: 600;
            background: #1f4e79; color: #fff; text-align: center; }
table { width: 100%; border-collapse: collapse; }
th, td { border: 1px solid #b4b4b4; padding: 3px 6px; }
th { background: #1f4e79; color: #fff; font-weight: 600; text-align: center; font-size: 11px; }
td.name { text-align: left; }
td.c, td.n { text-align: right; white-space: nowrap; }
tr.total td { background: #d9e2f3; font-weight: 600; }
.loading { opacity: 0.5; pointer-events: none; }
</style>
</head>
<body>
<header><h1>Отчёт по браку — write_offs</h1></header>
<div class="toolbar">
  <fieldset class="group">
    <legend>Корпус / WH</legend>
    <div class="building-btns" id="buildingBtns"></div>
    <div class="wh-grid" id="whGrid"></div>
  </fieldset>
  <fieldset class="group weeks">
    <legend>Недели (ISO)</legend>
    <label>Год <input type="number" id="year" value="2026"></label>
    <label>Расчёт: пред. <select id="weekPrev"></select></label>
    <label>посл. <select id="weekLast"></select></label>
    <span class="hint">Все недели года — в таблице (прокрутка). Динамика, доля и среднее — только по двум выбранным неделям.</span>
  </fieldset>
  <div class="actions">
    <button type="button" id="btnRefreshData" class="primary">Обновить данные</button>
    <button type="button" id="btnApply" class="secondary">Применить</button>
    <button type="button" id="btnAllWh" class="secondary">Все WH</button>
  </div>
</div>
<div id="status">Загрузка…</div>
<div class="grid" id="reportGrid"></div>
<script>
const CONFIG = __CONFIG_JSON__;
const CATALOG = CONFIG.wh_catalog && CONFIG.wh_catalog.length
  ? CONFIG.wh_catalog
  : (CONFIG.wh_list || []).map(w => ({ wh_id: w.wh_id, name: String(w.wh_id), corpus: 0 }));
const ALL_WH_IDS = CATALOG.map(w => w.wh_id);

let selectedWh = new Set();
let activeBuilding = 'custom';
let availableWeeks = [];

function whLabel(id) {
  const w = CATALOG.find(x => x.wh_id === id);
  return w ? (id + ' — ' + w.name) : String(id);
}

function init() {
  if (CONFIG.week_year) document.getElementById('year').value = CONFIG.week_year;
  const grid = document.getElementById('whGrid');
  let lastCorpus = null;
  CATALOG.forEach(w => {
    if (w.corpus && w.corpus !== lastCorpus) {
      const hdr = document.createElement('div');
      hdr.className = 'corpus-hdr';
      hdr.textContent = w.corpus + ' корпус';
      grid.appendChild(hdr);
      lastCorpus = w.corpus;
    }
    const lab = document.createElement('label');
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.value = w.wh_id;
    cb.checked = true;
    cb.addEventListener('change', () => {
      activeBuilding = 'custom';
      syncBuildingButtons();
      const id = parseInt(cb.value, 10);
      if (cb.checked) selectedWh.add(id); else selectedWh.delete(id);
    });
    selectedWh.add(w.wh_id);
    lab.append(cb, document.createTextNode(' ' + whLabel(w.wh_id)));
    grid.appendChild(lab);
  });

  const btns = document.getElementById('buildingBtns');
  CONFIG.buildings.forEach(b => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.textContent = b.name;
    btn.dataset.id = b.id;
    btn.addEventListener('click', () => selectBuilding(b));
    btns.appendChild(btn);
  });

  document.getElementById('year').addEventListener('change', () => refreshWeeks().then(loadReport));
  document.getElementById('weekPrev').addEventListener('change', loadReport);
  document.getElementById('weekLast').addEventListener('change', loadReport);
  document.getElementById('btnApply').onclick = loadReport;
  document.getElementById('btnRefreshData').onclick = refreshData;
  document.getElementById('btnAllWh').onclick = async () => {
    selectedWh = new Set(ALL_WH_IDS);
    document.querySelectorAll('#whGrid input').forEach(cb => cb.checked = true);
    activeBuilding = 'all';
    syncBuildingButtons();
    await refreshWeeks();
    loadReport();
  };

  refreshWeeks().then(() => {
    if (CONFIG.week_prev && CONFIG.week_last && availableWeeks.length) {
      fillWeekSelects(availableWeeks, CONFIG.week_prev, CONFIG.week_last);
    }
    if (CONFIG.buildings.length) selectBuilding(CONFIG.buildings[0]);
    else loadReport();
  });
}

function fillWeekSelects(weeks, prev, last) {
  const selPrev = document.getElementById('weekPrev');
  const selLast = document.getElementById('weekLast');
  selPrev.innerHTML = '';
  selLast.innerHTML = '';
  weeks.forEach(w => {
    const o1 = document.createElement('option');
    o1.value = w; o1.textContent = w;
    const o2 = o1.cloneNode(true);
    selPrev.appendChild(o1);
    selLast.appendChild(o2);
  });
  if (weeks.length >= 2) {
    selPrev.value = String(prev ?? weeks[weeks.length - 2]);
    selLast.value = String(last ?? weeks[weeks.length - 1]);
  } else if (weeks.length === 1) {
    selPrev.value = selLast.value = String(weeks[0]);
  }
}

async function refreshWeeks() {
  const year = document.getElementById('year').value;
  const wh = selectedWh.size ? Array.from(selectedWh).join(',') : '';
  const q = new URLSearchParams({ year });
  if (wh) q.set('wh_ids', wh);
  try {
    const r = await fetch('/api/weeks?' + q);
    if (!r.ok) return;
    const data = await r.json();
    availableWeeks = data.weeks || [];
    const prev = parseInt(document.getElementById('weekPrev').value, 10);
    const last = parseInt(document.getElementById('weekLast').value, 10);
    fillWeekSelects(availableWeeks, prev, last);
  } catch (_) {}
}

async function selectBuilding(b) {
  activeBuilding = b.id;
  syncBuildingButtons();
  if (!b.wh_ids || b.wh_ids.length === 0) {
    selectedWh = new Set(ALL_WH_IDS);
    document.querySelectorAll('#whGrid input').forEach(cb => cb.checked = true);
  } else {
    selectedWh = new Set(b.wh_ids);
    document.querySelectorAll('#whGrid input').forEach(cb => {
      cb.checked = selectedWh.has(parseInt(cb.value, 10));
    });
  }
  await refreshWeeks();
  loadReport();
}

function syncBuildingButtons() {
  document.querySelectorAll('#buildingBtns button').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.id === activeBuilding);
  });
}

function selectedWeeks() {
  let wp = document.getElementById('weekPrev').value;
  let wl = document.getElementById('weekLast').value;
  if (!wp || !wl) {
    if (availableWeeks.length >= 2) {
      wp = String(availableWeeks[availableWeeks.length - 2]);
      wl = String(availableWeeks[availableWeeks.length - 1]);
    } else {
      wp = String(CONFIG.week_prev || 20);
      wl = String(CONFIG.week_last || 21);
    }
    fillWeekSelects(availableWeeks.length ? availableWeeks : [parseInt(wp,10)], parseInt(wp,10), parseInt(wl,10));
  }
  return { wp, wl };
}

async function parseApiResponse(r) {
  const raw = await r.text();
  if (!r.ok) {
    try {
      const err = JSON.parse(raw);
      throw new Error(err.error || raw);
    } catch (e) {
      if (e instanceof Error && e.message !== raw) throw e;
      throw new Error(raw || r.statusText);
    }
  }
  return JSON.parse(raw);
}

async function refreshData() {
  const status = document.getElementById('status');
  const grid = document.getElementById('reportGrid');
  const btn = document.getElementById('btnRefreshData');
  btn.disabled = true;
  grid.classList.add('loading');
  status.textContent = 'Обновление базы и загрузка отчёта…';
  const wh = selectedWh.size ? Array.from(selectedWh).join(',') : '';
  const year = document.getElementById('year').value;
  const q = new URLSearchParams({ year });
  if (wh) q.set('wh_ids', wh);
  try {
    const data = await parseApiResponse(await fetch('/api/refresh?' + q, { method: 'POST' }));
    if (data.weeks && data.weeks.length) {
      availableWeeks = data.weeks;
      const wp = data.week_prev ?? availableWeeks[availableWeeks.length - 2];
      const wl = data.week_last ?? availableWeeks[availableWeeks.length - 1];
      fillWeekSelects(availableWeeks, wp, wl);
    } else {
      await refreshWeeks();
    }
    await loadReport();
    const parts = [
      'Данные обновлены',
      data.row_count != null ? data.row_count.toLocaleString('ru') + ' строк' : '',
      data.max_date ? 'посл. дата ' + data.max_date : '',
    ].filter(Boolean);
    if (data.refresh_note) parts.push(data.refresh_note);
    status.textContent = parts.join(' · ');
  } catch (e) {
    status.textContent = 'Ошибка обновления: ' + e.message;
  } finally {
    btn.disabled = false;
    grid.classList.remove('loading');
  }
}

async function loadReport() {
  const status = document.getElementById('status');
  const grid = document.getElementById('reportGrid');
  grid.classList.add('loading');
  const wh = selectedWh.size ? Array.from(selectedWh).join(',') : '';
  const { wp, wl } = selectedWeeks();
  const q = new URLSearchParams({
    year: document.getElementById('year').value,
    week_prev: wp,
    week_last: wl,
  });
  if (wh) q.set('wh_ids', wh);
  status.textContent = 'Загрузка…';
  try {
    const data = await parseApiResponse(await fetch('/api/report?' + q));
    grid.innerHTML = data.html;
    const sorted = Array.from(selectedWh).sort((a,b)=>a-b);
    const label = selectedWh.size === ALL_WH_IDS.length
      ? 'все корпуса (' + ALL_WH_IDS.length + ' WH)'
      : sorted.map(whLabel).join('; ');
    status.textContent = `WH: ${label} · расчёт нед. ${data.week_prev}→${data.week_last}, в таблице: ${(data.weeks || []).join(', ')} (${data.year})`;
    if (data.weeks && data.weeks.length) {
      availableWeeks = data.weeks;
      const p = document.getElementById('weekPrev').value;
      const l = document.getElementById('weekLast').value;
      fillWeekSelects(data.weeks, parseInt(p, 10), parseInt(l, 10));
    }
  } catch (e) {
    status.textContent = 'Ошибка: ' + e.message;
  } finally {
    grid.classList.remove('loading');
  }
}

init();
</script>
</body>
</html>
"""


def register_routes(application) -> None:
    from datetime import datetime, timezone

    from flask import jsonify, request

    if getattr(application, "_brak_routes_registered", False):
        return
    application._brak_routes_registered = True

    def _cfg() -> dict:
        return load_config()

    def _resolve_wh_ids(cfg: dict) -> list[int] | None:
        wh_raw = request.args.get("wh_ids", "")
        catalog_ids = catalog_wh_ids(cfg)
        if wh_raw.strip():
            return [int(x) for x in wh_raw.split(",") if x.strip()]
        if catalog_ids:
            return catalog_ids
        return None

    @application.route("/")
    def index():
        try:
            cfg = _cfg()
            embed = {
                "wh_catalog": cfg.get("wh_catalog", []),
                "buildings": cfg.get("buildings", []),
                "week_year": cfg.get("week_year", 2026),
                "week_prev": cfg.get("week_prev", 20),
                "week_last": cfg.get("week_last", 21),
            }
            page = DASHBOARD_HTML.replace(
                "__CONFIG_JSON__", json.dumps(embed, ensure_ascii=False)
            )
            return page
        except Exception as exc:
            return f"<pre>Index error: {exc}</pre>", 500

    @application.route("/api/refresh", methods=["POST"])
    def api_refresh():
        env_err = check_db_env()
        if env_err:
            return jsonify({"error": env_err}), 503

        try:
            cfg = _cfg()
            year = request.args.get("year", cfg.get("week_year", 2026), type=int)
            wh_ids = _resolve_wh_ids(cfg)
            office_id = cfg.get("office_id")

            refresh_note = run_db_refresh()
            stats = fetch_db_stats(office_id, wh_ids)
            weeks = fetch_available_weeks(year, office_id, wh_ids)
            week_prev = cfg.get("week_prev", 20)
            week_last = cfg.get("week_last", 21)
            if len(weeks) >= 2:
                week_prev, week_last = weeks[-2], weeks[-1]

            return jsonify(
                {
                    "ok": True,
                    "refreshed_at": datetime.now(timezone.utc).isoformat(),
                    "refresh_note": refresh_note,
                    "row_count": stats["row_count"],
                    "max_date": stats["max_date"],
                    "total_amount": stats["total_amount"],
                    "year": year,
                    "weeks": weeks,
                    "week_prev": week_prev,
                    "week_last": week_last,
                }
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @application.route("/api/report")
    def api_report():
        env_err = check_db_env()
        if env_err:
            return jsonify({"error": env_err}), 503

        try:
            cfg = _cfg()
            year = request.args.get("year", cfg.get("week_year", 2026), type=int)

            def _week_arg(name: str, default: int) -> int:
                raw = request.args.get(name, "")
                if raw is None or str(raw).strip() == "":
                    return default
                try:
                    return int(raw)
                except (TypeError, ValueError):
                    return default

            week_prev = _week_arg("week_prev", cfg.get("week_prev", 20))
            week_last = _week_arg("week_last", cfg.get("week_last", 21))
            wh_ids = _resolve_wh_ids(cfg)
            office_id = cfg.get("office_id")
            data = build_report_data(wh_ids, office_id, year, week_prev, week_last)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

        weeks = data["weeks"]
        html_grid = (
            render_table(
                "Дефект ТОП-20, рубли",
                data["defects"],
                data["defects_total"],
                show_id=True,
                weeks=weeks,
                week_prev=week_prev,
                week_last=week_last,
                name_header="Дефект",
            )
            + render_table(
                "Дефект ТОП-20, ORG 0, рубли",
                data["defects_org0"],
                data["defects_org0_total"],
                show_id=True,
                weeks=weeks,
                week_prev=week_prev,
                week_last=week_last,
                name_header="Дефект",
            )
            + render_table(
                "ТОП-20 категорий, рубли",
                data["categories"],
                data["categories_total"],
                show_id=False,
                weeks=weeks,
                week_prev=week_prev,
                week_last=week_last,
                name_header="Категория",
            )
            + render_table(
                "ТОП-20 категорий, ORG 0, рубли",
                data["categories_org0"],
                data["categories_org0_total"],
                show_id=False,
                weeks=weeks,
                week_prev=week_prev,
                week_last=week_last,
                name_header="Категория",
            )
        )

        return jsonify(
            {
                "html": html_grid,
                "year": year,
                "weeks": weeks,
                "week_prev": week_prev,
                "week_last": week_last,
            }
        )

    @application.route("/api/weeks")
    def api_weeks():
        env_err = check_db_env()
        if env_err:
            return jsonify({"error": env_err}), 503
        try:
            cfg = _cfg()
            year = request.args.get("year", cfg.get("week_year", 2026), type=int)
            wh_ids = _resolve_wh_ids(cfg)
            weeks = fetch_available_weeks(year, cfg.get("office_id"), wh_ids)
            week_prev = cfg.get("week_prev", 20)
            week_last = cfg.get("week_last", 21)
            if len(weeks) >= 2:
                week_prev, week_last = weeks[-2], weeks[-1]
            elif len(weeks) == 1:
                week_prev = week_last = weeks[0]
            return jsonify(
                {
                    "year": year,
                    "weeks": weeks,
                    "week_prev": week_prev,
                    "week_last": week_last,
                }
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @application.route("/api/wh_ids")
    def api_wh_ids():
        return jsonify(fetch_wh_list(_cfg().get("office_id")))

    @application.route("/health")
    def health():
        return jsonify({"status": "ok", "config": str(CONFIG_PATH.name)})

    @application.route("/health/db")
    def health_db():
        env_err = check_db_env()
        if env_err:
            return jsonify({"status": "error", "detail": env_err}), 503
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
            return jsonify({"status": "ok", "database": "connected"})
        except Exception as exc:
            return jsonify({"status": "error", "detail": str(exc)}), 503


def create_app():
    from flask import Flask

    application = Flask(__name__)
    register_routes(application)
    return application


def run_server() -> None:
    host = os.environ.get("HTML_HOST", "127.0.0.1")
    port = int(os.environ.get("HTML_PORT", "8080"))
    print(f"Дашборд: http://{host}:{port}/")
    print(f"Корпуса: {CONFIG_PATH}")
    create_app().run(host=host, port=port, debug=False)


def main() -> int:
    if not os.environ.get("DB_PASSWORD"):
        print("Создайте .env с DB_PASSWORD", file=sys.stderr)
        return 1
    try:
        run_server()
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
