def report_pipeline_status(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT state, COUNT(*) FROM file_state GROUP BY state")
    counts = cursor.fetchall()

    print("\n--- Pipeline Status ---")
    for state, count in counts:
        print(f"  {state}: {count}")
    print("-----------------------\n")


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