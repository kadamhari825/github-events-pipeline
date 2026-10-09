import sqlite3
import time

from gh_events.config import DB_PATH
from gh_events.spark_session import get_spark
from gh_events.state_store import init_state_db, discover_files
from gh_events.monitoring import report_pipeline_status, report_failed_files
from gh_events.stages.extract import run_extract_stage
from gh_events.stages.schema_validation import run_schema_validation_stage
from gh_events.stages.quality_check import run_quality_checked_stage
from gh_events.stages.transform import run_transformed_stage
from gh_events.stages.publish import run_published_stage



def main():
    run_extract_stage()
    spark = get_spark()
    init_state_db()
    discover_files()
    conn = sqlite3.connect(DB_PATH)
    timings = {}
    for stage_name, stage_fn in [
        ("schema_validation", run_schema_validation_stage),
        ("quality_checked", run_quality_checked_stage),
        ("transformed", run_transformed_stage),
        ("published", run_published_stage),
    ]:
        t0 = time.time()
        stage_fn(spark, conn)
        timings[stage_name] = time.time() - t0

    print("\n--- Stage timings (seconds) ---")
    for stage_name, secs in timings.items():
        print(f"  {stage_name}: {secs:.1f}")
    print(f"  total: {sum(timings.values()):.1f}")

    report_pipeline_status(conn)
    report_failed_files(conn)

    conn.close()
    spark.stop()


if __name__ == "__main__":
    main()