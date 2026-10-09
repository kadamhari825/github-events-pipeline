import os

from pyspark.sql.functions import (
    hour, to_timestamp, col, count, countDistinct,
    min as spark_min, max as spark_max,
    row_number, dense_rank,
)
from pyspark.sql.window import Window

from gh_events.spark_session import get_spark
from gh_events.config import DB_DIR


def main():
    spark = get_spark(app_name="GH_Archive_Analysis")
    df = spark.read.parquet(os.path.join(DB_DIR, "published"))
    df.printSchema()

    print("1. Find the count of type by descending")
    df.groupBy('type').count().orderBy('count', ascending=False).show(30)

    print("2. Find the top 30 repo names")
    df.groupBy('repo.name').count().orderBy('count', ascending=False).show(30)

    print("3. What was the busiest hour of the day")
    df.withColumn('event_hour', hour(to_timestamp("created_at"))) \
        .groupBy('event_hour') \
        .count() \
        .orderBy("event_hour", ascending=False) \
        .show(24)

    print("4. Show me the most active actors")
    df.groupBy('actor.login').count().orderBy('count', ascending=False).show(30)

    print("5. In push events, which repos had the most pushes")
    df.filter(df.type == 'PushEvent').groupBy('repo.name').count() \
        .orderBy('count', ascending=False).show(30)

    print("6. Event type share and window function usage")
    df.createOrReplaceTempView("GitHub_arch")
    spark.sql("""
        select type, count(*) * 100.0 / (select count(*) from GitHub_arch) as pct
        from GitHub_arch
        group by type
    """).show(10)

    w = Window.partitionBy("actor.id").orderBy(col("created_at").desc())
    df.withColumn('rank', dense_rank().over(w)).filter(col('rank') == 2).show(13, truncate=False)

    print("7. Show me aggregation usage")
    df.groupBy("type").agg(
        count("*").alias("event_count"),
        countDistinct("actor.login").alias("unique_actors"),
        countDistinct("repo.name").alias("unique_repos"),
        spark_min("created_at").alias("earliest_event"),
        spark_max("created_at").alias("latest_event")
    ).orderBy(col("event_count").desc()).show(30, truncate=False)

    print("8. Rank top 5 repos within each event type")
    df5 = df.groupBy("type", "repo.name").count()
    w5 = Window.partitionBy('type').orderBy(col('count').desc())
    df5.withColumn('rank', row_number().over(w5)).filter(col('rank') <= 5).show(45)

    spark.stop()


if __name__ == "__main__":
    main()