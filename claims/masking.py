"""PHI de-identification in the spirit of HIPAA Safe Harbor: drop direct identifiers,
pseudonymize member ids with a salted hash, generalize DOB and ZIP."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DIRECT_IDENTIFIERS = ["first_name", "last_name", "ssn"]


def pseudonymize(col: str, salt: str):
    return F.sha2(F.concat(F.lit(salt), F.col(col)), 256)


def mask_members(members: DataFrame, salt: str) -> DataFrame:
    return (members
            .withColumn("member_key", pseudonymize("member_id", salt))
            .withColumn("birth_year", F.year(F.to_date("dob")))
            .withColumn("zip3", F.substring("zip", 1, 3))
            .drop(*DIRECT_IDENTIFIERS, "member_id", "dob", "zip"))
