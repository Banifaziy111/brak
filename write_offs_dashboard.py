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
    w_prev: float
    w_last: float

    @property
    def dynamics(self) -> float | None:
        if self.w_prev == 0:
            return None if self.w_last == 0 else 100.0
        return (self.w_last - self.w_prev) / self.w_prev * 100

    @property
    def average(self) -> float:
        return (self.w_prev + self.w_last) / 2

    @property
    def pct_vs_avg(self) -> float | None:
        if self.average == 0:
            return None
        return self.w_last / self.average * 100


def db_config() -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME", "botdb"),
        "user": os.environ.get("DB_USER", ""),
        "password": os.environ.get("DB_PASSWORD", ""),
        "connect_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT", "15")),
    }
    sslmode = os.environ.get("DB_SSLMODE")
    if sslmode:
        cfg["sslmode"] = sslmode
    return cfg


def check_db_env() -> str | None:
    missing = [k for k in ("DB_HOST", "DB_USER", "DB_PASSWORD") if not os.environ.get(k)]
    if missing:
        return "Не заданы переменные Vercel: " + ", ".join(missing)
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


def fetch_top(
    *,
    group_col: str,
    id_col: str | None,
    wh_ids: list[int] | None,
    office_id: int | None,
    org0_only: bool,
    year: int,
    week_prev: int,
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
    id_select = f"{id_col} AS row_id," if id_col else "NULL::int AS row_id,"
    name_expr = group_col

    sql = f"""
        SELECT
            {id_select}
            COALESCE({name_expr}, '—') AS name,
            COALESCE(SUM(amount) FILTER (
                WHERE EXTRACT(WEEK FROM date) = %s
            ), 0) AS w_prev,
            COALESCE(SUM(amount) FILTER (
                WHERE EXTRACT(WEEK FROM date) = %s
            ), 0) AS w_last
        FROM brak_team.write_offs
        {where}
        GROUP BY {id_col + ',' if id_col else ''} {name_expr}
        ORDER BY w_last DESC NULLS LAST, w_prev DESC NULLS LAST
        LIMIT %s
    """
    # %s в FILTER идут в тексте раньше, чем в WHERE
    qparams = [week_prev, week_last, *params, limit]

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, qparams)
        raw = cur.fetchall()

    return [
        Row(
            row_id=int(r[0]) if r[0] is not None else None,
            name=str(r[1]),
            w_prev=to_float(r[2]),
            w_last=to_float(r[3]),
        )
        for r in raw
    ]


def add_shares(rows: list[Row]) -> list[dict]:
    total_last = sum(r.w_last for r in rows)
    out: list[dict] = []
    for i, r in enumerate(rows, start=1):
        share = (r.w_last / total_last * 100) if total_last else 0
        out.append(
            {
                "num": i,
                "row_id": r.row_id,
                "name": r.name,
                "w_prev": r.w_prev,
                "w_last": r.w_last,
                "dynamics": r.dynamics,
                "share": share,
                "average": r.average,
                "pct_vs_avg": r.pct_vs_avg,
            }
        )
    return out


def totals(rows: list[dict]) -> dict:
    w_prev = sum(x["w_prev"] for x in rows)
    w_last = sum(x["w_last"] for x in rows)
    avg = (w_prev + w_last) / 2 if rows else 0
    dyn = ((w_last - w_prev) / w_prev * 100) if w_prev else None
    pct_avg = (w_last / avg * 100) if avg else None
    return {
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


def render_table(
    title: str,
    rows: list[dict],
    total: dict,
    *,
    show_id: bool,
    week_prev: int,
    week_last: int,
) -> str:
    id_hdr = "<th>ИД</th>" if show_id else "<th>№</th>"
    body = []
    for r in rows:
        id_cell = (
            f"<td class='c'>{r['row_id']}</td>"
            if show_id
            else f"<td class='c'>{r['num']}</td>"
        )
        body.append(
            f"<tr>{id_cell}"
            f"<td class='name'>{_e(r['name'])}</td>"
            f"<td class='n'>{fmt_num(r['w_prev'])}</td>"
            f"<td class='n'>{fmt_num(r['w_last'])}</td>"
            f"<td class='n' style='{_e(heat_style(r['dynamics'], 'dynamics'))}'>{fmt_pct(r['dynamics'])}</td>"
            f"<td class='n' style='{_e(heat_style(r['share'], 'share'))}'>{fmt_pct(r['share'])}</td>"
            f"<td class='n'>{fmt_num(r['average'])}</td>"
            f"<td class='n' style='{_e(heat_style(r['pct_vs_avg'], 'pct_vs_avg'))}'>{fmt_pct(r['pct_vs_avg'])}</td>"
            "</tr>"
        )

    t = total
    body.append(
        f"<tr class='total'>"
        f"<td colspan='2'><b>Итого</b></td>"
        f"<td class='n'><b>{fmt_num(t['w_prev'])}</b></td>"
        f"<td class='n'><b>{fmt_num(t['w_last'])}</b></td>"
        f"<td class='n' style='{_e(heat_style(t['dynamics'], 'dynamics'))}'><b>{fmt_pct(t['dynamics'])}</b></td>"
        f"<td class='n'><b>{fmt_pct(t['share'])}</b></td>"
        f"<td class='n'><b>{fmt_num(t['average'])}</b></td>"
        f"<td class='n' style='{_e(heat_style(t['pct_vs_avg'], 'pct_vs_avg'))}'><b>{fmt_pct(t['pct_vs_avg'])}</b></td>"
        f"</tr>"
    )

    return f"""
<section class="panel">
  <h2>{_e(title)}</h2>
  <table>
    <thead>
      <tr>
        {id_hdr}
        <th>Дефект/Неделя</th>
        <th class="n">{week_prev}</th>
        <th class="n">{week_last}</th>
        <th>Динамика последней недели к предыдущей</th>
        <th>Доля от общего брака</th>
        <th>Среднее за 2 недели</th>
        <th>% последней недели от средней</th>
      </tr>
    </thead>
    <tbody>{''.join(body)}</tbody>
  </table>
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
) -> dict:
    defects = fetch_top(
        group_col="reason_descr",
        id_col="reason_id",
        wh_ids=wh_ids,
        office_id=office_id,
        org0_only=False,
        year=year,
        week_prev=week_prev,
        week_last=week_last,
    )
    defects_org0 = fetch_top(
        group_col="reason_descr",
        id_col="reason_id",
        wh_ids=wh_ids,
        office_id=office_id,
        org0_only=True,
        year=year,
        week_prev=week_prev,
        week_last=week_last,
    )
    cats = fetch_top(
        group_col="parent_name",
        id_col=None,
        wh_ids=wh_ids,
        office_id=office_id,
        org0_only=False,
        year=year,
        week_prev=week_prev,
        week_last=week_last,
    )
    cats_org0 = fetch_top(
        group_col="parent_name",
        id_col=None,
        wh_ids=wh_ids,
        office_id=office_id,
        org0_only=True,
        year=year,
        week_prev=week_prev,
        week_last=week_last,
    )

    d_rows = add_shares(defects)
    d0_rows = add_shares(defects_org0)
    c_rows = add_shares(cats)
    c0_rows = add_shares(cats_org0)

    return {
        "defects": d_rows,
        "defects_total": totals(d_rows),
        "defects_org0": d0_rows,
        "defects_org0_total": totals(d0_rows),
        "categories": c_rows,
        "categories_total": totals(c_rows),
        "categories_org0": c0_rows,
        "categories_org0_total": totals(c0_rows),
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
.weeks input { width: 56px; padding: 4px; margin-right: 8px; }
.actions { display: flex; align-items: flex-end; gap: 8px; }
.actions button { padding: 8px 20px; background: #217346; color: #fff; border: none;
  border-radius: 4px; cursor: pointer; font-weight: 600; }
.actions button:hover { background: #1a5c38; }
#status { padding: 8px 16px; color: #555; font-size: 12px; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; padding: 12px; }
@media (max-width: 1200px) { .grid { grid-template-columns: 1fr; } }
.panel { background: #fff; border: 1px solid #999; overflow: auto; }
.panel h2 { margin: 0; padding: 8px 10px; font-size: 13px; font-weight: 600;
            background: #1f4e79; color: #fff; text-align: center; }
table { width: 100%; border-collapse: collapse; }
th, td { border: 1px solid #b4b4b4; padding: 3px 6px; }
th { background: #1f4e79; color: #fff; font-weight: 600; text-align: center; font-size: 11px; }
td.name { text-align: left; max-width: 200px; }
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
    <label>Пред. <input type="number" id="weekPrev" value="20" min="1" max="53"></label>
    <label>Посл. <input type="number" id="weekLast" value="21" min="1" max="53"></label>
  </fieldset>
  <div class="actions">
    <button type="button" id="btnApply">Обновить</button>
    <button type="button" id="btnAllWh">Все WH</button>
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

function whLabel(id) {
  const w = CATALOG.find(x => x.wh_id === id);
  return w ? (id + ' — ' + w.name) : String(id);
}

function init() {
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

  document.getElementById('btnApply').onclick = loadReport;
  document.getElementById('btnAllWh').onclick = () => {
    selectedWh = new Set(ALL_WH_IDS);
    document.querySelectorAll('#whGrid input').forEach(cb => cb.checked = true);
    activeBuilding = 'all';
    syncBuildingButtons();
    loadReport();
  };

  if (CONFIG.buildings.length) selectBuilding(CONFIG.buildings[0]);
  else loadReport();
}

function selectBuilding(b) {
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
  loadReport();
}

function syncBuildingButtons() {
  document.querySelectorAll('#buildingBtns button').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.id === activeBuilding);
  });
}

async function loadReport() {
  const status = document.getElementById('status');
  const grid = document.getElementById('reportGrid');
  grid.classList.add('loading');
  const wh = selectedWh.size ? Array.from(selectedWh).join(',') : '';
  const q = new URLSearchParams({
    year: document.getElementById('year').value,
    week_prev: document.getElementById('weekPrev').value,
    week_last: document.getElementById('weekLast').value,
  });
  if (wh) q.set('wh_ids', wh);
  status.textContent = 'Загрузка…';
  try {
    const r = await fetch('/api/report?' + q);
    if (!r.ok) throw new Error(await r.text());
    const data = await r.json();
    grid.innerHTML = data.html;
    const sorted = Array.from(selectedWh).sort((a,b)=>a-b);
    const label = selectedWh.size === ALL_WH_IDS.length
      ? 'все корпуса (' + ALL_WH_IDS.length + ' WH)'
      : sorted.map(whLabel).join('; ');
    status.textContent = `WH: ${label} · недели ${data.week_prev}/${data.week_last} ${data.year}`;
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
    from flask import jsonify, request

    cfg = load_config()

    @application.route("/")
    def index():
        embed = {
            "wh_catalog": cfg.get("wh_catalog", []),
            "buildings": cfg.get("buildings", []),
        }
        page = DASHBOARD_HTML.replace("__CONFIG_JSON__", json.dumps(embed, ensure_ascii=False))
        return page

    @application.route("/api/report")
    def api_report():
        env_err = check_db_env()
        if env_err:
            return jsonify({"error": env_err}), 503

        try:
            year = request.args.get("year", cfg.get("week_year", 2026), type=int)
            week_prev = request.args.get("week_prev", cfg.get("week_prev", 20), type=int)
            week_last = request.args.get("week_last", cfg.get("week_last", 21), type=int)
            wh_raw = request.args.get("wh_ids", "")
            catalog_ids = catalog_wh_ids(cfg)
            wh_ids: list[int] | None
            if wh_raw.strip():
                wh_ids = [int(x) for x in wh_raw.split(",") if x.strip()]
            elif catalog_ids:
                wh_ids = catalog_ids
            else:
                wh_ids = None

            office_id = cfg.get("office_id")
            data = build_report_data(wh_ids, office_id, year, week_prev, week_last)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

        html_grid = (
            render_table(
                "Дефект ТОП-20, рубли",
                data["defects"],
                data["defects_total"],
                show_id=True,
                week_prev=week_prev,
                week_last=week_last,
            )
            + render_table(
                "Дефект ТОП-20, ORG 0, рубли",
                data["defects_org0"],
                data["defects_org0_total"],
                show_id=True,
                week_prev=week_prev,
                week_last=week_last,
            )
            + render_table(
                "ТОП-20 категорий, рубли",
                data["categories"],
                data["categories_total"],
                show_id=False,
                week_prev=week_prev,
                week_last=week_last,
            )
            + render_table(
                "ТОП-20 категорий, ORG 0, рубли",
                data["categories_org0"],
                data["categories_org0_total"],
                show_id=False,
                week_prev=week_prev,
                week_last=week_last,
            )
        )

        return jsonify(
            {
                "html": html_grid,
                "year": year,
                "week_prev": week_prev,
                "week_last": week_last,
            }
        )

    @application.route("/api/wh_ids")
    def api_wh_ids():
        return jsonify(fetch_wh_list(cfg.get("office_id")))

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
