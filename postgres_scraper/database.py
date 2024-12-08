# database.py
import psycopg2

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "1234",
    "dbname": "postgres"
}


def fetch_pg_stat_database():
    query = """
        SELECT 
            numbackends,
            xact_commit,
            xact_rollback,
            blks_read,
            blks_hit,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            conflicts,
            temp_files,
            temp_bytes,
            deadlocks
        FROM pg_stat_database 
        WHERE datname = %s;
    """
    return execute_query(query, (POSTGRES_CONFIG['dbname'],))


def fetch_pg_stat_user_tables():
    query = """
        SELECT 
            relname,
            seq_scan,
            seq_tup_read,
            idx_scan,
            idx_tup_fetch,
            n_tup_ins,
            n_tup_upd,
            n_tup_del,
            n_live_tup,
            n_dead_tup,
            vacuum_count,
            autovacuum_count
        FROM pg_stat_user_tables;
    """
    return execute_query(query)


def fetch_pg_stat_activity():
    query = """
        SELECT 
            pid,
            usename,
            state,
            query,
            backend_start,
            state_change
        FROM pg_stat_activity;
    """
    return execute_query(query)


def fetch_pg_stat_bgwriter():
    query = """
        SELECT 
            checkpoints_timed,
            checkpoints_req,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc
        FROM pg_stat_bgwriter;
    """
    return execute_query(query)


def fetch_pg_locks():
    query = """
        SELECT 
            locktype,
            mode,
            granted,
            pid
        FROM pg_locks;
    """
    return execute_query(query)


def fetch_pg_stat_statements():
    query = """
        SELECT 
            query,
            calls,
            total_time,
            rows,
            shared_blks_hit,
            shared_blks_read
        FROM pg_stat_statements
        ORDER BY total_time DESC
        LIMIT 10;
    """
    return execute_query(query)


def execute_query(query, params=None):
    try:
        with psycopg2.connect(**POSTGRES_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params) if params else cur.execute(query)
                result = cur.fetchall()
                return result
    except Exception as e:
        print(f"Error fetching metrics: {e}")
        return None


def fetch_all_metrics():
    metrics = {
        "pg_stat_database": fetch_pg_stat_database(),
        "pg_stat_user_tables": fetch_pg_stat_user_tables(),
        "pg_stat_activity": fetch_pg_stat_activity(),
        "pg_stat_bgwriter": fetch_pg_stat_bgwriter(),
        "pg_locks": fetch_pg_locks(),
        "pg_stat_statements": fetch_pg_stat_statements()
    }
    return metrics
