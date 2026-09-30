"""Run end to end: generate -> mask -> quality -> vault + star -> reconcile -> parquet.

python -m claims.pipeline --out output
"""
from __future__ import annotations

import argparse
import json
import os

from pyspark.sql import functions as F

from .generate import generate
from .masking import mask_members, pseudonymize
from .quality import apply_quality
from .reconcile import reconcile
from .spark import get_spark
from .star import build_star
from .vault import build_vault


def run(spark, out: str | None = None, run_date: str = "2026-01-01", salt: str | None = None) -> dict:
    salt = salt or os.getenv("PHI_SALT", "local-dev-salt")
    members, providers, claims, _ = generate()
    members_df = spark.createDataFrame(members)
    providers_df = spark.createDataFrame(providers)
    claims_df = spark.createDataFrame(claims, schema=(
        "claim_id string, member_id string, npi string, service_date string, paid_date string, "
        "icd10 string, cpt string, billed_amount double, paid_amount double"))

    valid, quarantine, stats = apply_quality(claims_df, members_df, run_date)
    masked_members = mask_members(members_df, salt)
    valid_masked = valid.withColumn("member_key", pseudonymize("member_id", salt)).drop("member_id")

    vault = build_vault(valid_masked, masked_members, providers_df, load_ts=f"{run_date} 00:00:00")
    star = build_star(valid_masked, masked_members, providers_df)
    report = reconcile(claims_df.dropDuplicates(), valid, quarantine, star["fact_claim"])
    reasons = (quarantine.select(F.explode("dq_reasons").alias("reason"))
               .groupBy("reason").count().orderBy(F.desc("count")).collect())
    result = {"stats": stats, "reconciliation": report,
              "quarantine_reasons": {r["reason"]: r["count"] for r in reasons}}
    if out:
        for name, df in {**vault, **star, "quarantine": quarantine}.items():
            df.write.mode("overwrite").parquet(f"{out}/{name}")
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="output")
    ap.add_argument("--run-date", default="2026-01-01")
    a = ap.parse_args()
    print(json.dumps(run(get_spark(), a.out, a.run_date), indent=2, default=str))
