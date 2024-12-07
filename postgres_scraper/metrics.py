# metrics.py
import time

from postgres_scraper.database import fetch_pg_stat_database

# Global variable to store the previous xact_commit and timestamp
previous_xact_commit = None
previous_timestamp = None


def get_pg_metrics():
    """Fetch metrics from pg_stat_database and return them as a dict."""
    global previous_xact_commit, previous_timestamp

    result = fetch_pg_stat_database()
    if not result:
        return None

    # Unpack the result in the order of the SELECT statement
    (numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
     tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
     conflicts, temp_files, temp_bytes, deadlocks) = result

    current_timestamp = time.time()

    # Calculate commits per second
    if previous_xact_commit is not None and previous_timestamp is not None:
        time_diff = current_timestamp - previous_timestamp
        commits_per_second = (xact_commit - previous_xact_commit) / time_diff if time_diff > 0 else 0
    else:
        commits_per_second = 0

    # Update the previous values for the next call
    previous_xact_commit = xact_commit
    previous_timestamp = current_timestamp

    return {
        'timestamp': current_timestamp,
        'active_connections': numbackends,
        'xact_commit': xact_commit,
        'xact_rollback': xact_rollback,
        'blks_read': blks_read,
        'blks_hit': blks_hit,
        'tup_returned': tup_returned,
        'tup_fetched': tup_fetched,
        'tup_inserted': tup_inserted,
        'tup_updated': tup_updated,
        'tup_deleted': tup_deleted,
        'conflicts': conflicts,
        'temp_files': temp_files,
        'temp_bytes': temp_bytes,
        'deadlocks': deadlocks,
        'commits_per_second': commits_per_second
    }