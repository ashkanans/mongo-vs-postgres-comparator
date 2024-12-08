# metrics.py
import time

from postgres_scraper.database import fetch_pg_stat_database, fetch_pg_stat_user_tables, fetch_pg_stat_activity, \
    fetch_pg_stat_bgwriter, fetch_pg_locks, fetch_pg_stat_statements

# Global variable to store the previous xact_commit and timestamp
previous_xact_commit = None
previous_timestamp = None


def get_pg_metrics():
    """Fetch all relevant PostgreSQL metrics and return them as a comprehensive dictionary."""
    global previous_xact_commit, previous_timestamp

    # Fetch metrics from different views
    pg_stat_database = fetch_pg_stat_database()
    pg_stat_user_tables = fetch_pg_stat_user_tables()
    pg_stat_activity = fetch_pg_stat_activity()
    pg_stat_bgwriter = fetch_pg_stat_bgwriter()
    pg_locks = fetch_pg_locks()
    pg_stat_statements = fetch_pg_stat_statements()

    current_timestamp = time.time()

    # Process pg_stat_database metrics
    if pg_stat_database:
        (numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
         tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
         conflicts, temp_files, temp_bytes, deadlocks) = pg_stat_database[0]

        # Calculate commits per second
        if previous_xact_commit is not None and previous_timestamp is not None:
            time_diff = current_timestamp - previous_timestamp
            commits_per_second = (xact_commit - previous_xact_commit) / time_diff if time_diff > 0 else 0
        else:
            commits_per_second = 0

        # Update the previous values for the next call
        previous_xact_commit = xact_commit
        previous_timestamp = current_timestamp

        # Base metrics from pg_stat_database
        metrics = {
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
    else:
        metrics = {'error': 'Failed to fetch pg_stat_database metrics'}

    # Process pg_stat_user_tables metrics
    if pg_stat_user_tables:
        metrics['user_tables_stats'] = []
        for row in pg_stat_user_tables:
            (relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd,
             n_tup_del, n_live_tup, n_dead_tup, vacuum_count, autovacuum_count) = row

            table_stats = {
                'table_name': relname,
                'seq_scan': seq_scan,
                'seq_tup_read': seq_tup_read,
                'idx_scan': idx_scan,
                'idx_tup_fetch': idx_tup_fetch,
                'n_tup_ins': n_tup_ins,
                'n_tup_upd': n_tup_upd,
                'n_tup_del': n_tup_del,
                'n_live_tup': n_live_tup,
                'n_dead_tup': n_dead_tup,
                'vacuum_count': vacuum_count,
                'autovacuum_count': autovacuum_count
            }

            metrics['user_tables_stats'].append(table_stats)

    # Add additional metrics to the dictionary
    metrics.update({
        'pg_stat_user_tables': pg_stat_user_tables,
        'pg_stat_activity': pg_stat_activity,
        'pg_stat_bgwriter': pg_stat_bgwriter,
        'pg_locks': pg_locks,
        'pg_stat_statements': pg_stat_statements
    })

    return metrics
