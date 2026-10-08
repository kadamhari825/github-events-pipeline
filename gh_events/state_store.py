import glob
import os
import sqlite3
from datetime import datetime

from gh_events import config


def init_state_db(db_path=config.DB_PATH):
    conn = sqlite3.connect(db_path)
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
    print(f"database ready at {db_path}")


def discover_files(db_path=config.DB_PATH, raw_dir=config.RAW_DIR):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    raw_files = glob.glob(os.path.join(raw_dir, "*.json.gz"))
    for file_path in raw_files:
        file_name = os.path.basename(file_path)
        cursor.execute(
            "INSERT OR IGNORE INTO file_state (file_name, state) values (?, ?)",
            (file_name, "DISCOVERED"),
        )

    conn.commit()

    cursor.execute("SELECT file_name, state from file_state")
    for row in cursor.fetchall():
        print(row)

    conn.close()


def handle_stage_failure(cursor, file_name, error_msg, retry_count):
    """
    Called from inside a stage's except block. Decides whether to retry
    or give up, based on how many times this file has already failed.
    """
    if retry_count < config.MAX_RETRIES:
        new_retry_count = retry_count + 1
        cursor.execute(
            "UPDATE file_state SET retry_count = ?, error_message = ?, updated_at = ? WHERE file_name = ?",
            (new_retry_count, error_msg, datetime.now().isoformat(), file_name),
        )
        print(f"{file_name}: attempt failed ({error_msg}), retry {new_retry_count}/{config.MAX_RETRIES} - state left unchanged for reprocessing")
    else:
        cursor.execute(
            "UPDATE file_state SET state = ?, error_message = ?, updated_at = ? WHERE file_name = ?",
            ("FAILED", error_msg, datetime.now().isoformat(), file_name),
        )
        print(f"{file_name}: FAILED permanently after {retry_count} retries - {error_msg}")