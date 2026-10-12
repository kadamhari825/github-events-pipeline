import sqlite3

from gh_events.state_store import init_state_db, handle_stage_failure


def make_db(tmp_path, file_name, state, retry_count):
    db_path = str(tmp_path / "test_state.db")
    init_state_db(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO file_state (file_name, state, retry_count) VALUES (?, ?, ?)",
        (file_name, state, retry_count),
    )
    conn.commit()
    return conn, cursor


def test_first_failure_adds_one_retry_and_keeps_state(tmp_path):
    conn, cursor = make_db(tmp_path, "2026-08-30-00.json.gz", "DISCOVERED", 0)

    handle_stage_failure(cursor, "2026-08-30-00.json.gz", "disk hiccup", 0)

    cursor.execute(
        "SELECT state, retry_count, error_message FROM file_state WHERE file_name = ?",
        ("2026-08-30-00.json.gz",),
    )
    state, retry_count, error_message = cursor.fetchone()
    assert state == "DISCOVERED"
    assert retry_count == 1
    assert error_message == "disk hiccup"
    conn.close()

def test_second_failure_adds_another_retry_and_keeps_state(tmp_path):
    conn, cursor = make_db(tmp_path, "2026-08-30-01.json.gz", "DISCOVERED", 1)

    handle_stage_failure(cursor, "2026-08-30-01.json.gz", "lock timeout", 1)

    cursor.execute(
        "SELECT state, retry_count, error_message FROM file_state WHERE file_name = ?",
        ("2026-08-30-01.json.gz",),
    )
    state, retry_count, error_message = cursor.fetchone()
    assert state == "DISCOVERED"
    assert retry_count == 2
    assert error_message == "lock timeout"
    conn.close()


def test_failure_at_retry_limit_marks_file_failed(tmp_path):
    conn, cursor = make_db(tmp_path, "2026-08-30-02.json.gz", "DISCOVERED", 2)

    handle_stage_failure(cursor, "2026-08-30-02.json.gz", "still broken", 2)

    cursor.execute(
        "SELECT state, retry_count, error_message FROM file_state WHERE file_name = ?",
        ("2026-08-30-02.json.gz",),
    )
    state, retry_count, error_message = cursor.fetchone()
    assert state == "FAILED"
    assert retry_count == 2
    assert error_message == "still broken"
    conn.close()