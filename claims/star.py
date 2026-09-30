"""Business-facing star schema built from the cleansed claims."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_star(claims: DataFrame, members: DataFrame, providers: DataFrame) -> dict[str, DataFrame]:
    dim_member = members.select("member_key", "birth_year", "zip3", "payer")
    dim_provider = providers.select("npi", "provider_name", "specialty", "state")
    dates = claims.select(F.to_date("service_date").alias("date")).distinct()
    dim_date = dates.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
        F.year("date").alias("year"), F.quarter("date").alias("quarter"),
        F.month("date").alias("month"), F.dayofweek("date").alias("day_of_week"))
    fact_claim = claims.select(
        "claim_id", "member_key", "npi",
        F.date_format(F.to_date("service_date"), "yyyyMMdd").cast("int").alias("service_date_key"),
        "icd10", "cpt", "billed_amount", "paid_amount",
        (F.col("billed_amount") - F.col("paid_amount")).alias("patient_responsibility"),
        F.datediff(F.to_date("paid_date"), F.to_date("service_date")).alias("days_to_pay"))
    return {"dim_member": dim_member, "dim_provider": dim_provider, "dim_date": dim_date,
            "fact_claim": fact_claim}
