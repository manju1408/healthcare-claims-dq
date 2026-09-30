from pyspark.sql import functions as F

from claims.generate import generate
from claims.masking import DIRECT_IDENTIFIERS, mask_members
from claims.vault import build_vault, new_satellite_rows

CLAIMS_SCHEMA = ("claim_id string, member_key string, npi string, service_date string, "
                 "paid_date string, icd10 string, cpt string, billed_amount double, paid_amount double")


def members(spark):
    return spark.createDataFrame(generate(n_members=20, n_claims=10)[0])


def sat_member(spark, masked, load_ts):
    no_claims = spark.createDataFrame([], CLAIMS_SCHEMA)
    providers = spark.createDataFrame([("1",)], "npi string")
    return build_vault(no_claims, masked, providers, load_ts)["sat_member"]


def test_no_direct_identifiers_after_masking(spark):
    masked = mask_members(members(spark), salt="s1")
    assert not set(DIRECT_IDENTIFIERS + ["dob", "zip", "member_id"]) & set(masked.columns)
    assert masked.filter(F.length("zip3") != 3).count() == 0


def test_pseudonyms_are_salted_and_stable(spark):
    def keys(salt):
        return [r[0] for r in mask_members(members(spark), salt).orderBy("member_key").select("member_key").collect()]

    assert keys("s1") == keys("s1")
    assert keys("s1") != keys("s2")


def test_satellite_only_inserts_changed_rows(spark):
    masked = mask_members(members(spark), "s1")
    day1 = sat_member(spark, masked, "2026-01-01 00:00:00")
    changed = masked.withColumn(
        "payer", F.when(F.col("birth_year") % 2 == 0, F.lit("NEW_PAYER")).otherwise(F.col("payer")))
    day2 = sat_member(spark, changed, "2026-01-02 00:00:00")

    delta = new_satellite_rows(day1, day2, "hk_member")
    expected = masked.filter(F.col("birth_year") % 2 == 0).count()
    assert expected > 0 and delta.count() == expected
    assert new_satellite_rows(day1, day1, "hk_member").count() == 0
