import os

from pyspark.sql import SparkSession


def get_spark(app_name="GH_Archive"):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )