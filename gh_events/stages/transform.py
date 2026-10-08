import os
from datetime import datetime

from pyspark.sql.functions import input_file_name

from gh_events import config
from gh_events.state_store import handle_stage_failure

def run_transformed_stage(spark, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT file_name, retry_count FROM file_state WHERE state = 'QUALITY_CHECKED'")
    files = cursor.fetchall()

    if not files:
        print("files already transformed")
        return

    file_names = [row[0] for row in files]
    retry_counts = {row[0]: row[1] for row in files}
    file_paths = [os.path.join(config.RAW_DIR, f) for f in file_names]

    try:
        df = spark.read.json(file_paths)
        transformed_df = df.select('id', 'type', 'actor', 'repo', 'created_at', input_file_name().alias('source_file'))
        print(f"Transformed row count: {transformed_df.count()}")

        transformed_df.write.mode('append').parquet(os.path.join(config.DB_DIR, 'transformed'))

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