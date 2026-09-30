from claims.generate import generate
from claims.quality import apply_quality

SCHEMA = ("claim_id string, member_id string, npi string, service_date string, paid_date string, "
          "icd10 string, cpt string, billed_amount double, paid_amount double")


def test_every_injected_defect_is_quarantined(spark):
    members, _, claims, injected = generate(n_claims=800, defect_rate=0.1)
    valid, quarantine, stats = apply_quality(spark.createDataFrame(claims, SCHEMA),
                                             spark.createDataFrame(members), "2026-01-01")
    q = {r["claim_id"]: r["dq_reasons"] for r in quarantine.collect()}
    assert set(q) == set(injected)
    assert all(injected[cid] in reasons for cid, reasons in q.items())
    assert stats["duplicates"] == 20
    assert stats["valid"] + stats["quarantined"] == stats["input"] - stats["duplicates"]


def test_valid_icd_codes_pass(spark):
    from pyspark.sql import functions as F
    from claims.quality import ICD10_PATTERN

    df = spark.createDataFrame([("E11.9",), ("I10",), ("J45.909",), ("99X",), ("U07",)], "code string")
    ok = [r["code"] for r in df.filter(F.col("code").rlike(ICD10_PATTERN)).collect()]
    assert ok == ["E11.9", "I10", "J45.909"]
