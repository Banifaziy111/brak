"""Pure analytics helpers for the write-offs dashboard (no DB I/O)."""

from __future__ import annotations

from typing import Any


def to_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _row_key(row: dict[str, Any], kind: str) -> str:
    if kind == "reason":
        rid = row.get("row_id")
        return f"r:{rid}" if rid is not None else f"n:{row.get('name')}"
    return f"c:{row.get('name')}"


def build_period_compare(
    rows: list[dict[str, Any]],
    week_a: int,
    week_b: int,
    *,
    kind: str = "reason",
    limit: int = 15,
) -> list[dict[str, Any]]:
    """Compare amounts for week_a vs week_b across report rows."""
    items: list[dict[str, Any]] = []
    for row in rows or []:
        amounts = row.get("amounts") or {}
        a = to_float(amounts.get(week_a, row.get("w_prev") if week_a else 0))
        b = to_float(amounts.get(week_b, row.get("w_last") if week_b else 0))
        # Prefer explicit week keys; fall back only when comparing prev/last fields.
        if week_a in amounts or week_b in amounts:
            a = to_float(amounts.get(week_a, 0))
            b = to_float(amounts.get(week_b, 0))
        delta = b - a
        pct = ((b / a - 1.0) * 100) if a else (100.0 if b else None)
        items.append(
            {
                "kind": kind,
                "row_id": row.get("row_id"),
                "name": row.get("name") or "—",
                "week_a": week_a,
                "week_b": week_b,
                "amount_a": a,
                "amount_b": b,
                "delta": delta,
                "pct": pct,
            }
        )
    items.sort(key=lambda x: (-abs(x["delta"]), -x["amount_b"]))
    return items[: max(1, min(50, limit))]


def build_top20_churn(
    prev_rows: list[dict[str, Any]],
    last_rows: list[dict[str, Any]],
    *,
    kind: str = "reason",
) -> dict[str, Any]:
    """Classify TOP-20 membership and rank changes between two snapshots."""
    prev_sorted = sorted(
        prev_rows or [],
        key=lambda r: -to_float(r.get("w_last") or r.get("amount") or 0),
    )
    last_sorted = sorted(
        last_rows or [],
        key=lambda r: -to_float(r.get("w_last") or r.get("amount") or 0),
    )
    prev_rank = {_row_key(r, kind): i + 1 for i, r in enumerate(prev_sorted)}
    last_rank = {_row_key(r, kind): i + 1 for i, r in enumerate(last_sorted)}
    prev_map = {_row_key(r, kind): r for r in prev_sorted}
    last_map = {_row_key(r, kind): r for r in last_sorted}
    prev_keys = set(prev_map)
    last_keys = set(last_map)

    def _item(key: str, status: str) -> dict[str, Any]:
        src = last_map.get(key) or prev_map.get(key) or {}
        if status == "exited":
            prev_amt = to_float(src.get("w_last") or src.get("amount") or 0)
            last_amt = 0.0
        elif status == "entered":
            last_amt = to_float(src.get("w_last") or src.get("amount") or 0)
            prev_amt = 0.0
        else:
            prev_amt = to_float((prev_map.get(key) or {}).get("w_last") or (prev_map.get(key) or {}).get("amount") or 0)
            last_amt = to_float((last_map.get(key) or {}).get("w_last") or (last_map.get(key) or {}).get("amount") or 0)
        rp = prev_rank.get(key)
        rl = last_rank.get(key)
        rank_delta = None
        if rp is not None and rl is not None:
            # Positive = moved up in ranking (e.g. 5 -> 2 => +3).
            rank_delta = rp - rl
        return {
            "kind": kind,
            "status": status,
            "row_id": src.get("row_id"),
            "name": src.get("name") or "—",
            "amount_prev": prev_amt,
            "amount_last": last_amt,
            "delta": last_amt - prev_amt,
            "rank_prev": rp,
            "rank_last": rl,
            "rank_delta": rank_delta,
        }

    entered = [_item(k, "entered") for k in (last_keys - prev_keys)]
    exited = [_item(k, "exited") for k in (prev_keys - last_keys)]
    stayed = [_item(k, "stayed") for k in (prev_keys & last_keys)]
    entered.sort(key=lambda x: -x["amount_last"])
    exited.sort(key=lambda x: -x["amount_prev"])
    stayed.sort(key=lambda x: -abs(x["delta"]))
    rank_up = sorted(
        [x for x in stayed if (x.get("rank_delta") or 0) > 0],
        key=lambda x: (-x["rank_delta"], -abs(x["delta"])),
    )
    rank_down = sorted(
        [x for x in stayed if (x.get("rank_delta") or 0) < 0],
        key=lambda x: (x["rank_delta"], -abs(x["delta"])),
    )
    return {
        "entered": entered,
        "exited": exited,
        "stayed": stayed,
        "rank_up": rank_up,
        "rank_down": rank_down,
        "membership_changed": bool(entered or exited),
    }


def build_growth_alerts(
    report: dict,
    *,
    min_dynamics: float = 15.0,
    min_vs_avg4: float = 20.0,
    min_amount: float = 50000.0,
    limit: int = 8,
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for kind, key, label in (
        ("reason", "defects", "Дефект"),
        ("category", "categories", "Категория"),
    ):
        for row in report.get(key) or []:
            dyn = row.get("dynamics")
            last = to_float(row.get("w_last"))
            prev = to_float(row.get("w_prev"))
            avg4 = to_float(row.get("avg4"))
            vs_avg4 = row.get("vs_avg4")
            wow_hit = dyn is not None and dyn >= min_dynamics and last >= min_amount
            avg_hit = (
                vs_avg4 is not None
                and to_float(vs_avg4) >= min_vs_avg4
                and last >= min_amount
                and avg4 > 0
            )
            if not (wow_hit or avg_hit):
                continue
            alerts.append(
                {
                    "kind": kind,
                    "label": label,
                    "row_id": row.get("row_id"),
                    "name": row.get("name") or "—",
                    "w_prev": prev,
                    "w_last": last,
                    "dynamics": to_float(dyn) if dyn is not None else None,
                    "avg4": avg4,
                    "vs_avg4": to_float(vs_avg4) if vs_avg4 is not None else None,
                    "delta": last - prev,
                    "share": to_float(row.get("share")),
                    "trigger": "vs_avg4" if avg_hit and not wow_hit else "wow",
                    "alert_key": _row_key(row, kind),
                }
            )
    alerts.sort(
        key=lambda a: (
            -(a["vs_avg4"] if a.get("vs_avg4") is not None else a.get("dynamics") or 0),
            -a["w_last"],
        )
    )
    return alerts[: max(1, min(20, limit))]


def concentration_shares(
    items: list[dict[str, Any]],
    *,
    total: float,
    top_n: int,
    amount_key: str = "amount",
) -> float:
    if total <= 0:
        return 0.0
    top = sorted(items or [], key=lambda x: -to_float(x.get(amount_key)))[: max(0, top_n)]
    return sum(to_float(x.get(amount_key)) for x in top) / total * 100


def yoy_pct(current: float, previous: float) -> float | None:
    if previous <= 0:
        return 100.0 if current > 0 else None
    return (current / previous - 1.0) * 100


def parse_year_value(raw: Any, default: int | None = None) -> int | None:
    if raw is None or str(raw).strip() == "":
        return default
    try:
        year = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("Некорректный year: значение должно быть числом") from exc
    if year < 2000 or year > 2100:
        raise ValueError("Некорректный year: допустим диапазон 2000–2100")
    return year


def stable_etag_payload(payload: dict[str, Any]) -> str:
    import hashlib
    import json

    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()
