import os
from datetime import datetime

from gh_events import config
from gh_events.state_store import handle_stage_failure


def run_quality_checked_stage(spark, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT file_name, retry_count FROM file_state WHERE state = 'SCHEMA_VALIDATED'")
    files = cursor.fetchall()

    for file_name, retry_count in files:
        file_path = os.path.join(config.RAW_DIR, file_name)

        try:
            df = spark.read.json(file_path)
            null_id_count = df.filter(df.id.isNull()).count()
            null_created_at_count = df.filter(df.created_at.isNull()).count()
            invalid_type_count = df.filter(~df.type.isin(config.KNOWN_EVENT_TYPES)).count()

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
            # only genuine crashes retry, because the quality findings above are deterministic
            handle_stage_failure(cursor, file_name, str(e), retry_count)

    conn.commit()