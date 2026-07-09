from brak_dashboard.analytics import (
    build_growth_alerts,
    build_period_compare,
    build_top20_churn,
    concentration_shares,
    parse_year_value,
    stable_etag_payload,
    yoy_pct,
)


def test_build_period_compare_orders_by_abs_delta():
    rows = [
        {"row_id": 1, "name": "A", "amounts": {10: 100, 11: 110}},
        {"row_id": 2, "name": "B", "amounts": {10: 50, 11: 200}},
    ]
    out = build_period_compare(rows, 10, 11, kind="reason", limit=10)
    assert out[0]["name"] == "B"
    assert out[0]["delta"] == 150
    assert round(out[0]["pct"], 1) == 300.0


def test_build_top20_churn_entered_exited_stayed():
    prev = [
        {"row_id": 1, "name": "Keep", "w_last": 100},
        {"row_id": 2, "name": "Out", "w_last": 80},
    ]
    last = [
        {"row_id": 1, "name": "Keep", "w_last": 120},
        {"row_id": 3, "name": "In", "w_last": 90},
    ]
    churn = build_top20_churn(prev, last, kind="reason")
    assert [x["name"] for x in churn["entered"]] == ["In"]
    assert [x["name"] for x in churn["exited"]] == ["Out"]
    assert churn["stayed"][0]["name"] == "Keep"
    assert churn["stayed"][0]["delta"] == 20
    assert churn["membership_changed"] is True


def test_build_top20_churn_rank_fallback_when_same_members():
    prev = [
        {"row_id": 1, "name": "A", "w_last": 200},
        {"row_id": 2, "name": "B", "w_last": 100},
    ]
    last = [
        {"row_id": 2, "name": "B", "w_last": 180},
        {"row_id": 1, "name": "A", "w_last": 150},
    ]
    churn = build_top20_churn(prev, last, kind="reason")
    assert churn["entered"] == []
    assert churn["exited"] == []
    assert churn["membership_changed"] is False
    assert churn["rank_up"][0]["name"] == "B"
    assert churn["rank_up"][0]["rank_delta"] == 1
    assert churn["rank_down"][0]["name"] == "A"
    assert churn["rank_down"][0]["rank_delta"] == -1


def test_build_growth_alerts_thresholds_and_key():
    report = {
        "defects": [
            {
                "row_id": 7,
                "name": "X",
                "w_prev": 100000,
                "w_last": 130000,
                "dynamics": 30,
                "avg4": 100000,
                "vs_avg4": 30,
                "share": 10,
            }
        ],
        "categories": [],
    }
    alerts = build_growth_alerts(report, min_dynamics=15, min_vs_avg4=20, min_amount=50000)
    assert len(alerts) == 1
    assert alerts[0]["alert_key"] == "r:7"
    none = build_growth_alerts(report, min_dynamics=50, min_vs_avg4=50, min_amount=50000)
    assert none == []


def test_concentration_and_yoy():
    assert concentration_shares(
        [{"amount": 60}, {"amount": 30}, {"amount": 10}], total=100, top_n=2
    ) == 90
    assert round(yoy_pct(120, 100), 6) == 20.0
    assert yoy_pct(50, 0) == 100.0
    assert yoy_pct(0, 0) is None


def test_parse_year_value():
    assert parse_year_value("", 2026) == 2026
    assert parse_year_value("2025") == 2025
    try:
        parse_year_value("abc")
        assert False, "expected ValueError"
    except ValueError:
        pass
    try:
        parse_year_value("1999")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_stable_etag_payload():
    a = stable_etag_payload({"b": 1, "a": 2})
    b = stable_etag_payload({"a": 2, "b": 1})
    assert a == b
    assert len(a) == 40
