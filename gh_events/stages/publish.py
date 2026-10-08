import os
from datetime import datetime

from pyspark.sql.functions import col, regexp_extract

from gh_events import config
from gh_events.state_store import handle_stage_failure


def run_published_stage(spark, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT file_name, retry_count FROM file_state WHERE state = 'TRANSFORMED'")
    files = cursor.fetchall()

    if not files:
        print('files are not ready for publishing')
        return

    file_names = [row[0] for row in files]
    retry_counts = {row[0]: row[1] for row in files}
    published_path = os.path.join(config.DB_DIR, 'published')

    try:
        transformed_df = spark.read.parquet(os.path.join(config.DB_DIR, 'transformed'))

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
            print("all candidate files already present in /published - nothing new to write")

        for file_name in original_candidates:
            cursor.execute(
                "UPDATE file_state SET state = ?, retry_count = 0, updated_at = ? WHERE file_name = ?",
                ('PUBLISHED', datetime.now().isoformat(), file_name)
            )

    except Exception as e:
        for file_name in file_names:
            handle_stage_failure(cursor, file_name, str(e), retry_counts[file_name])

    conn.commit()