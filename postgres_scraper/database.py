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
    try:
        with psycopg2.connect(**POSTGRES_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (POSTGRES_CONFIG['dbname'],))
                result = cur.fetchone()
                return result
    except Exception as e:
        print(f"Error fetching metrics: {e}")
        return None
