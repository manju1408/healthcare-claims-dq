import pytest

from claims.pipeline import run
from claims.reconcile import ReconciliationError, reconcile


def test_end_to_end_reconciles(spark, tmp_path):
    result = run(spark, out=str(tmp_path / "out"))
    checks = result["reconciliation"]["checks"]
    assert all(checks.values())
    assert result["stats"]["quarantined"] > 0 and result["quarantine_reasons"]
    for table in ["hub_claim", "sat_claim", "link_claim_member_provider", "fact_claim", "dim_date", "quarantine"]:
        assert (tmp_path / "out" / table).exists()


def test_reconciliation_fails_when_rows_are_lost(spark):
    src = spark.createDataFrame([("a", 10.0), ("b", 5.0)], "claim_id string, paid_amount double")
    valid = src.limit(1)
    empty = src.limit(0)
    with pytest.raises(ReconciliationError):
        reconcile(src, valid, empty, valid)
