"""Raw Data Vault 2.0 structures: hubs, a link and satellites with hash keys and hashdiffs."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def hash_key(*cols: str):
    return F.sha2(F.concat_ws("||", *[F.upper(F.trim(F.col(c).cast("string"))) for c in cols]), 256)


def _meta(df: DataFrame, source: str, load_ts: str) -> DataFrame:
    return df.withColumn("load_ts", F.lit(load_ts).cast("timestamp")).withColumn("record_source", F.lit(source))


def build_vault(claims: DataFrame, members: DataFrame, providers: DataFrame,
                load_ts: str, source: str = "claims_feed") -> dict[str, DataFrame]:
    hub_member = _meta(members.select("member_key").distinct()
                       .withColumn("hk_member", hash_key("member_key")), source, load_ts)
    hub_provider = _meta(providers.select("npi").distinct().withColumn("hk_provider", hash_key("npi")),
                         source, load_ts)
    hub_claim = _meta(claims.select("claim_id").distinct().withColumn("hk_claim", hash_key("claim_id")),
                      source, load_ts)
    link = _meta(claims.select("claim_id", "member_key", "npi").distinct()
                 .withColumn("hk_claim", hash_key("claim_id"))
                 .withColumn("hk_member", hash_key("member_key"))
                 .withColumn("hk_provider", hash_key("npi"))
                 .withColumn("hk_link", hash_key("claim_id", "member_key", "npi")), source, load_ts)
    sat_cols = ["service_date", "paid_date", "icd10", "cpt", "billed_amount", "paid_amount"]
    sat_claim = _meta(claims.select("claim_id", *sat_cols)
                      .withColumn("hk_claim", hash_key("claim_id"))
                      .withColumn("hashdiff", hash_key(*sat_cols)), source, load_ts)
    sat_member = _meta(members.withColumn("hk_member", hash_key("member_key"))
                       .withColumn("hashdiff", hash_key("birth_year", "zip3", "payer")), source, load_ts)
    return {"hub_member": hub_member, "hub_provider": hub_provider, "hub_claim": hub_claim,
            "link_claim_member_provider": link, "sat_claim": sat_claim, "sat_member": sat_member}


def new_satellite_rows(existing: DataFrame, incoming: DataFrame, key: str) -> DataFrame:
    """Insert-only satellite load: keep incoming rows whose hashdiff differs from the latest."""
    latest = existing.groupBy(key).agg(F.max_by("hashdiff", "load_ts").alias("_latest"))
    return (incoming.join(latest, key, "left")
            .filter(F.col("_latest").isNull() | (F.col("_latest") != F.col("hashdiff")))
            .drop("_latest"))
