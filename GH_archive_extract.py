import requests
import os
import glob
import sqlite3
import time
from pyspark.sql.functions import lit, coalesce
from datetime import datetime
from pyspark.sql.functions import row_number, col, dense_rank
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import to_timestamp, hour, col, count, countDistinct, min as spark_min, max as spark_max, regexp_extract

date = '2026-08-30'
hours_to_download = [f"{date}-{h}" for h in range(24)]

RAW_DIR = "/Users/harikadam/gharchive_project/raw"
os.makedirs(RAW_DIR, exist_ok = True)

BASE_URL = "https://data.gharchive.org"

for hour_str in hours_to_download:
    file_name = f"{hour_str}.json.gz"
    url = f'{BASE_URL}/{file_name}'
    local_path = os.path.join(RAW_DIR, file_name)


    if os.path.exists(local_path):
        print(f"already have {local_path}, skipping")
        continue
    print(f"Downloading {url}")
    response = requests.get(url, stream = True)

    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                f.write(chunk)
        print(f"saved {file_name}")

    else:
        print(f'failed to download {file_name}:status {response.status_code}')
#---------------------------------------------------------------------------------------

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

spark = SparkSession.builder \
    .appName('GH_Archive') \
    .config('spark.driver.bindAddress', '127.0.0.1') \
    .config('spark.driver.host', '127.0.0.1') \
    .getOrCreate()
df = spark.read.json("/Users/harikadam/gharchive_project/raw/*.json.gz")

#---------------------------------------------------------------------------------------

DB_DIR = "/Users/harikadam/gharchive_project"
DB_PATH = os.path.join(DB_DIR, "pipeline_state.db")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS file_state(
file_name TEXT PRIMARY KEY,
state TEXT NOT NULL,
event_count INTEGER,
error_message TEXT,
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
retry_count INTEGER DEFAULT 0
)
""")

conn.commit()
conn.close()
print(f"database ready at {DB_PATH}")

#------------------------------------------------------------------------------------

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

raw_files = glob.glob(os.path.join(RAW_DIR, "*.json.gz"))
for file_path in raw_files:
    file_name = os.path.basename(file_path)

    cursor.execute("""
    INSERT OR IGNORE INTO file_state (file_name, state)
    values (?, ?)
    """, (file_name, 'DISCOVERED'))

conn.commit()

cursor.execute("SELECT file_name, state from file_state ")
rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()


#---------------------------------------------------------------
# Retry Function

MAX_RETRIES = 2

def handle_stage_failure(cursor, file_name, error_msg, retry_count):
    """
    Called from inside a stage's except block. Decides whether to retry
    or give up, based on how many times this file has already failed.
    """
    if retry_count < MAX_RETRIES:
        new_retry_count = retry_count + 1
        cursor.execute(
            "UPDATE file_state SET retry_count = ?, error_message = ?, updated_at = ? WHERE file_name = ?",
            (new_retry_count, error_msg, datetime.now().isoformat(), file_name)
        )
        print(f"{file_name}: attempt failed ({error_msg}), retry {new_retry_count}/{MAX_RETRIES} — state left unchanged for reprocessing")
    else:
        cursor.execute(
            "UPDATE file_state SET state = ?, error_message = ?, updated_at = ? WHERE file_name = ?",
            ("FAILED", error_msg, datetime.now().isoformat(), file_name)
        )
        print(f"{file_name}: FAILED permanently after {retry_count} retries — {error_msg}")


#----------------------------------------------------
# Schema vlidation

EXPECTED_FIELDS = {'id', 'type', 'actor', 'repo', 'created_at'}

def run_schema_validation_stage(spark, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT file_name, retry_count FROM file_state WHERE state = 'DISCOVERED'")
    files = cursor.fetchall()

    for file_name, retry_count in files:
        file_path = os.path.join(RAW_DIR, file_name)

        try:
            df = spark.read.json(file_path)
            count = df.count()
            actual_fields = set(df.columns)
            missing = EXPECTED_FIELDS - actual_fields

            if missing:
                # structural problem, not transient — no point retrying
                error_msg = f"Missing fields: {sorted(missing)}"
                cursor.execute(
                    "UPDATE file_state SET state = ?, event_count = ?, error_message = ?, updated_at = ? WHERE file_name = ?",
                    ("FAILED", count, error_msg, datetime.now().isoformat(), file_name)
                )
                print(f"{file_name}: FAILED schema validation (permanent, no retry) - {error_msg}")
            else:
                cursor.execute(
                    "UPDATE file_state SET state = ?, event_count = ?, updated_at = ? WHERE file_name = ?",
                    ("SCHEMA_VALIDATED", count, datetime.now().isoformat(), file_name)
                )
                print(f"{file_name}: SCHEMA_VALIDATED with {count} events")

        except Exception as e:
            # could be transient (disk hiccup, lock) — worth retrying
            handle_stage_failure(cursor, file_name, str(e), retry_count)

    conn.commit()

#----------------------------------------------
# quality checked

KNOWN_EVENT_TYPES = {
    'CommitCommentEvent', 'CreateEvent', 'DeleteEvent', 'ForkEvent',
    'GollumEvent', 'IssueCommentEvent', 'IssuesEvent', 'MemberEvent',
    'PublicEvent', 'PullRequestEvent', 'PullRequestReviewEvent',
    'PullRequestReviewCommentEvent', 'PullRequestReviewThreadEvent',
    'PushEvent', 'ReleaseEvent', 'SponsorshipEvent', 'WatchEvent',
    'DiscussionEvent', 'DiscussionCommentEvent'
}

def run_quality_checked_stage(spark, conn):
    cursor = conn.cursor()
    cursor.execute('SELECT file_name, retry_count FROM file_state WHERE state = "SCHEMA_VALIDATED"')
    files = cursor.fetchall()

    for file_name, retry_count in files:
        file_path = os.path.join(RAW_DIR, file_name)

        try:
            df = spark.read.json(file_path)
            null_id_count = df.filter(df.id.isNull()).count()
            null_created_at_count = df.filter(df.created_at.isNull()).count()
            invalid_type_count = df.filter(~df.type.isin(KNOWN_EVENT_TYPES)).count()

            issues = []
            if null_id_count > 0:
                issues.append(f"{null_id_count} null id")
            if null_created_at_count > 0:
                issues.append(f"{null_created_at_count} null created_at")
            if invalid_type_count > 0:
                issues.append(f"{invalid_type_count} unknown type")

            quality_note = "; ".join(issues) if issues else None

            cursor.execute(
                "UPDATE file_state SET state = ?, error_message = ?, updated_at = ? WHERE file_name = ?",
                ("QUALITY_CHECKED", quality_note, datetime.now().isoformat(), file_name)
            )

            if quality_note:
                print(f"quality checked for {file_name}: flagged: {quality_note}")
            else:
                print(f"quality checked for {file_name} - clean")

        except Exception as e:
            # only genuine crashes retry — quality findings above are deterministic, not retried
            handle_stage_failure(cursor, file_name, str(e), retry_count)

    conn.commit()

#-------------------------------------
# Transformed

def run_transformed_stage(spark, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT file_name, retry_count FROM file_state WHERE state = 'QUALITY_CHECKED'")
    files = cursor.fetchall()

    if not files:
        print("files already transformed")
        return

    file_names = [row[0] for row in files]
    retry_counts = {row[0]: row[1] for row in files}
    file_paths = [os.path.join(RAW_DIR, f) for f in file_names]

    try:
        df = spark.read.json(file_paths)
        transformed_df = df.select('id', 'type', 'actor', 'repo', 'created_at', input_file_name().alias('source_file'))
        print(f"Transformed row count: {transformed_df.count()}")

        transformed_df.write.mode('append').parquet(os.path.join(DB_DIR, 'transformed'))

        for file_name in file_names:
            cursor.execute(
                "UPDATE file_state SET state = ?, updated_at = ? WHERE file_name = ?",
                ('TRANSFORMED', datetime.now().isoformat(), file_name)
            )
        print(f"transformed {len(file_names)} files into combined output")

    except Exception as e:
        for file_name in file_names:
            handle_stage_failure(cursor, file_name, str(e), retry_counts[file_name])

    conn.commit()

#---------------------------------
# Published

def run_published_stage(spark, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT file_name, retry_count FROM file_state WHERE state = 'TRANSFORMED'")
    files = cursor.fetchall()

    if not files:
        print('files are not ready for publishing')
        return

    file_names = [row[0] for row in files]
    retry_counts = {row[0]: row[1] for row in files}
    published_path = os.path.join(DB_DIR, 'published')

    try:
        transformed_df = spark.read.parquet(os.path.join(DB_DIR, 'transformed'))

        new_rows_df = transformed_df.withColumn(
            'source_file_name', regexp_extract(col('source_file'), r'([^/]+)$', 1)
        ).filter(col('source_file_name').isin(file_names))

        if os.path.exists(published_path):
            already_published_df = spark.read.parquet(published_path)
            already_published_files = [
                row.source_file_name for row in
                already_published_df.withColumn(
                    'source_file_name', regexp_extract(col('source_file'), r'([^/]+)$', 1)
                ).select('source_file_name').distinct().collect()
            ]
            file_names = [f for f in file_names if f not in already_published_files]
            new_rows_df = new_rows_df.filter(~col('source_file_name').isin(already_published_files))

        new_rows_df = new_rows_df.drop('source_file_name')

        original_candidates = [row[0] for row in files]

        if file_names:
            new_rows_df.write.mode("append").partitionBy("type").parquet(published_path)
            print(f"published {len(file_names)} files worth of data, partitioned by type")
        else:
            print("all candidate files already present in /published — nothing new to write")

        for file_name in original_candidates:
            cursor.execute(
                "UPDATE file_state SET state = ?, updated_at = ? WHERE file_name = ?",
                ('PUBLISHED', datetime.now().isoformat(), file_name)
            )

    except Exception as e:
        for file_name in file_names:
            handle_stage_failure(cursor, file_name, str(e), retry_counts[file_name])

    conn.commit()

# new idempotent code





#---------------------------------------------
# Monitoring

def report_pipeline_status(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT state, COUNT(*) FROM file_state GROUP BY state")
    counts = cursor.fetchall()

    print("\n--- Pipeline Status ---")
    for state, count in counts:
        print(f"  {state}: {count}")
    print("-----------------------\n")
#___________________________
# Alerting

def report_failed_files(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT file_name, retry_count, error_message, updated_at FROM file_state WHERE state = 'FAILED'")
    failed = cursor.fetchall()

    if not failed:
        print("No permanently failed files.")
        return

    print(f"\n⚠️  {len(failed)} file(s) permanently FAILED:")
    for file_name, retry_count, error_message, updated_at in failed:
        print(f"  - {file_name} (failed after {retry_count} retries, last attempt {updated_at}): {error_message}")
    print()

conn = sqlite3.connect(DB_PATH)
run_schema_validation_stage(spark, conn)
run_quality_checked_stage(spark, conn)
run_transformed_stage(spark, conn)
run_published_stage(spark, conn)

report_pipeline_status(conn)
report_failed_files(conn)

conn.close()
#_____________________________________
# Analysis using Pyspark

df = spark.read.parquet("/Users/harikadam/gharchive_project/published")
df.printSchema()

print("1. Find the count of type by decending ")
df.groupBy('type').count().orderBy('count', ascending = False).show(30)

print("2. Find the top 30 repo names")
df.groupBy('repo.name').count().orderBy('count', ascending = False).show(30)

print('3. What was busiest hour of the Day')
df.withColumn('event_hour', hour(to_timestamp("created_at"))) \
 .groupBy('event_hour') \
 .count() \
 .orderBy("event_hour", ascending = False) \
 .show(24)

print('4. Show me most active actors')
df.groupBy('actor.login').count().orderBy('count', ascending = False).show(30)

print('5. in push events who were these actors')
df.filter(df.type == 'PushEvent').groupBy('repo.name').count().orderBy('count', ascending = False).show(30)

print('6. show me repo which has most done push or pull and usge of window function')
df.createTempView("GitHub_arch")
spark.sql(""" select type, count(*) * 100.0 / ( select count(*) from GitHub_arch)  as pct from GitHub_arch 
              group by type """).show(10)

w = Window.partitionBy("actor.id").orderBy(col("created_at").desc())
df.withColumn('rank', dense_rank().over(w)).filter(col('rank') == 2).show(13, truncate= False)

print('7. show me aggregation usage')
df.groupBy("type").agg(
    count("*").alias("event_count"),
    countDistinct("actor.login").alias("unique_actors"),
    countDistinct("repo.name").alias("unique_repos"),
    spark_min("created_at").alias("earliest_event"),
    spark_max("created_at").alias("latest_event")
).orderBy(col("event_count").desc()).show(30, truncate=False)

print('8. Rabk top 5 event by repo name')

df5 = df.groupBy("type", "repo.name").count()
w = Window.partitionBy('type').orderBy(col('count').desc())
df5.withColumn('rank', row_number().over(w)).filter(col('rank') <=5 ).show(45)



spark.stop()








