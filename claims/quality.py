"""Declarative quality rules. Rows failing any rule are quarantined with every reason."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ICD10_PATTERN = r"^[A-TV-Z][0-9][0-9AB](\.[0-9A-TV-Z]{1,4})?$"


def rules(run_date: str) -> dict:
    svc, paid = F.to_date("service_date"), F.to_date("paid_date")
    return {
        "missing_claim_id": F.col("claim_id").isNull(),
        "missing_npi": F.col("npi").isNull(),
        "negative_amount": (F.col("billed_amount") < 0) | (F.col("paid_amount") < 0),
        "paid_gt_billed": F.col("paid_amount") > F.col("billed_amount"),
        "bad_icd": ~F.col("icd10").rlike(ICD10_PATTERN),
        "future_service": svc > F.lit(run_date).cast("date"),
        "paid_before_service": paid < svc,
        "orphan_member": F.col("_member_found").isNull(),
    }


def apply_quality(claims: DataFrame, members: DataFrame, run_date: str):
    """Returns (valid, quarantine, stats). Exact duplicates are dropped first."""
    deduped = claims.dropDuplicates()
    known = members.select("member_id", F.lit(True).alias("_member_found"))
    checked = deduped.join(known, "member_id", "left")
    flags = F.array(*[F.when(cond, F.lit(name)) for name, cond in rules(run_date).items()])
    reasons = F.filter(flags, lambda r: r.isNotNull())
    checked = checked.withColumn("dq_reasons", reasons).drop("_member_found")
    valid = checked.filter(F.size("dq_reasons") == 0).drop("dq_reasons")
    quarantine = checked.filter(F.size("dq_reasons") > 0).withColumn("quarantined_at", F.current_timestamp())
    stats = {"input": claims.count(), "duplicates": claims.count() - deduped.count(),
             "valid": valid.count(), "quarantined": quarantine.count()}
    return valid, quarantine, stats
