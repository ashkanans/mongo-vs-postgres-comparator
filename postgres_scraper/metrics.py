# metrics.py
import time

from postgres_scraper.database import fetch_pg_stat_database


def get_pg_metrics():
    """Fetch metrics from pg_stat_database and return them as a dict."""
    result = fetch_pg_stat_database()
    if not result:
        return None

    # Unpack the result in the order of the SELECT statement
    (numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
     tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
     conflicts, temp_files, temp_bytes, deadlocks) = result

    return {
        'timestamp': time.time(),
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
        'deadlocks': deadlocks
    }
