from pyspark.sql import SparkSession


def get_spark(app: str = "claims-dq") -> SparkSession:
    return (SparkSession.builder.appName(app).master("local[2]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate())
