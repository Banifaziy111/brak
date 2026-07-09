from brak_dashboard.analytics import stable_etag_payload


def test_etag_changes_when_payload_changes():
    e1 = stable_etag_payload({"week_last": 20, "kpis": {"total_last": 1}})
    e2 = stable_etag_payload({"week_last": 21, "kpis": {"total_last": 1}})
    assert e1 != e2
