import psycopg2

from dashboard.logger.logging_config import logger

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "1234",
    "dbname": "postgres"
}


class PostgresDataFetcher:
    def __init__(self, config=POSTGRES_CONFIG):
        self.config = config

    def _execute_query(self, query, params=None):
        try:
            logger.info(f"Executing query")
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cur:
                    if params:
                        cur.execute(query, params)
                    else:
                        cur.execute(query)
                    result = cur.fetchall()
                    logger.info(f"Query executed successfully")
                    return result
        except Exception as e:
            logger.error(f"Error executing query: {query} - {e}")
            return None

    def fetch_pg_stat_database(self, dbname=None):
        dbname = dbname or self.config['dbname']
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
        return self._execute_query(query, (dbname,))

    def fetch_pg_stat_user_tables(self):
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
        return self._execute_query(query)

    def fetch_pg_stat_activity(self):
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
        return self._execute_query(query)

    def fetch_pg_stat_bgwriter(self):
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
        return self._execute_query(query)

    def fetch_pg_locks(self):
        query = """
            SELECT 
                locktype,
                mode,
                granted,
                pid
            FROM pg_locks;
        """
        return self._execute_query(query)

    def fetch_pg_stat_statements(self):
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
        return self._execute_query(query)

    def fetch_pg_index_usage(self):
        query = """
            SELECT
                t.relname AS table_name,
                t.seq_scan,
                t.seq_tup_read,
                t.idx_scan,
                t.idx_tup_fetch,
                i.indexrelname AS index_name,
                i.idx_scan AS index_scans,
                i.idx_tup_read AS index_tuples_read
            FROM
                pg_stat_user_tables t
            JOIN
                pg_stat_user_indexes i ON t.relid = i.relid;
        """
        return self._execute_query(query)
