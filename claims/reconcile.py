"""Source-to-target reconciliation: counts and control totals must tie out."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ReconciliationError(Exception):
    pass


def _totals(df: DataFrame) -> tuple[int, float]:
    r = df.agg(F.count("*").alias("n"), F.round(F.sum("paid_amount"), 2).alias("paid")).first()
    return int(r["n"]), float(r["paid"] or 0.0)


def reconcile(deduped_source: DataFrame, valid: DataFrame, quarantine: DataFrame,
              fact: DataFrame, tolerance: float = 0.01) -> dict:
    src_n, src_paid = _totals(deduped_source)
    val_n, val_paid = _totals(valid)
    q_n, q_paid = _totals(quarantine)
    fact_n, fact_paid = _totals(fact)
    report = {
        "source_rows": src_n, "valid_rows": val_n, "quarantined_rows": q_n, "fact_rows": fact_n,
        "source_paid": src_paid, "valid_plus_quarantine_paid": round(val_paid + q_paid, 2),
        "fact_paid": fact_paid,
    }
    checks = {
        "rows_conserved": src_n == val_n + q_n,
        "paid_conserved": abs(src_paid - (val_paid + q_paid)) <= tolerance,
        "fact_matches_valid_rows": fact_n == val_n,
        "fact_matches_valid_paid": abs(fact_paid - val_paid) <= tolerance,
    }
    report["checks"] = checks
    if not all(checks.values()):
        raise ReconciliationError(report)
    return report
