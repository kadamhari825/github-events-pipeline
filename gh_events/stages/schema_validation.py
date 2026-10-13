import os
from datetime import datetime

from gh_events import config
from gh_events.state_store import handle_stage_failure


def compare_schema(flat_types, expected_schema):
    missing = [k for k in expected_schema if k not in flat_types]
    wrong_type = [f"{k} is {flat_types[k]}, expected {v}" for k, v in expected_schema.items() if k in flat_types and flat_types[k] != v]
    return missing, wrong_type


def run_schema_validation_stage(spark, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT file_name, retry_count FROM file_state WHERE state = 'DISCOVERED'")
    files = cursor.fetchall()

    for file_name, retry_count in files:
        file_path = os.path.join(config.RAW_DIR, file_name)

        try:
            df = spark.read.json(file_path)
            count = df.count()

            actual_fields = set(df.columns)
            extra = sorted(actual_fields - config.EXPECTED_FIELDS)

            flat_types = {}
            for f in df.schema.fields:
                flat_types[f.name] = f.dataType.simpleString()
                if f.dataType.typeName() == 'struct':
                    for inner in f.dataType.fields:
                        flat_types[f"{f.name}.{inner.name}"] = inner.dataType.simpleString()
            missing, wrong_type = compare_schema(flat_types, config.EXPECTED_SCHEMA)

            if missing or wrong_type:
                # structural problem, not transient, so no point retrying
                problems = []
                if missing:
                    problems.append(f"Missing fields: {sorted(missing)}")
                if wrong_type:
                    problems.append(f"Wrong types: {wrong_type}")
                error_msg = "; ".join(problems)

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
                print(f"{file_name}: SCHEMA_VALIDATED with {count} events | {len(actual_fields)} columns, {len(config.EXPECTED_FIELDS)} required, extra: {extra}")

        except Exception as e:
            # could be transient (disk hiccup, lock), so it is worth retrying
            handle_stage_failure(cursor, file_name, str(e), retry_count)

    conn.commit()