import time

from dashboard.data.postgres_data import PostgresDataFetcher
from dashboard.logger.logging_config import logger


class PostgresMetrics:
    def __init__(self):
        self.fetcher = PostgresDataFetcher()
        self.previous_xact_commit = None
        self.previous_timestamp = None

    def get_metrics(self):
        logger.info("Fetching PostgreSQL metrics...")
        current_timestamp = time.time()

        try:
            # Fetch data from different PostgreSQL views
            pg_stat_database = self.fetcher.fetch_pg_stat_database()
            pg_stat_user_tables = self.fetcher.fetch_pg_stat_user_tables()
            pg_stat_activity = self.fetcher.fetch_pg_stat_activity()
            pg_stat_bgwriter = self.fetcher.fetch_pg_stat_bgwriter()
            pg_locks = self.fetcher.fetch_pg_locks()
            pg_index_usage = self.fetcher.fetch_pg_index_usage()

            metrics = {'timestamp': current_timestamp}

            # Process pg_stat_database
            if pg_stat_database:
                logger.info("Processing pg_stat_database metrics")
                db_metrics = self._process_pg_stat_database(pg_stat_database[0], current_timestamp)
                metrics.update(db_metrics)
            else:
                logger.error("Failed to fetch pg_stat_database metrics")
                metrics['error'] = 'Failed to fetch pg_stat_database metrics'

            # Process pg_stat_user_tables
            if pg_stat_user_tables:
                logger.info("Processing pg_stat_user_tables metrics")
                metrics['user_tables_stats'] = self._process_pg_stat_user_tables(pg_stat_user_tables)

            # Process pg_stat_bgwriter
            if pg_stat_bgwriter:
                logger.info("Processing pg_stat_bgwriter metrics")
                metrics['bgwriter'] = self._process_pg_stat_bgwriter(pg_stat_bgwriter[0])

            if pg_index_usage:
                logger.info("Processing index usage metrics")
                metrics['index_usage'] = self._process_pg_index_usage(pg_index_usage)

            # Add raw data for other metrics
            metrics.update({
                'pg_stat_activity': pg_stat_activity,
                'pg_stat_bgwriter': pg_stat_bgwriter,
                'pg_locks': pg_locks
            })

            logger.info("Successfully fetched all metrics")
            return metrics

        except Exception as e:
            logger.error(f"Error fetching PostgreSQL metrics: {e}")
            return {'error': str(e)}

    def _process_pg_index_usage(self, index_usage_data):
        """Process index usage metrics."""
        index_usage_stats = []
        for row in index_usage_data:
            (table_name, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, index_name, index_scans,
             index_tuples_read) = row

            index_usage_stats.append({
                'table_name': table_name,
                'seq_scan': seq_scan,
                'seq_tup_read': seq_tup_read,
                'idx_scan': idx_scan,
                'idx_tup_fetch': idx_tup_fetch,
                'index_name': index_name,
                'index_scans': index_scans,
                'index_tuples_read': index_tuples_read
            })

        return index_usage_stats

    def _process_pg_stat_bgwriter(self, data):
        """Process pg_stat_bgwriter metrics."""
        (checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_clean,
         maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc) = data

        return {
            'checkpoints_timed': checkpoints_timed,
            'checkpoints_req': checkpoints_req,
            'buffers_checkpoint': buffers_checkpoint,
            'buffers_clean': buffers_clean,
            'maxwritten_clean': maxwritten_clean,
            'buffers_backend': buffers_backend,
            'buffers_backend_fsync': buffers_backend_fsync,
            'buffers_alloc': buffers_alloc
        }


    def _process_pg_stat_database(self, data, current_timestamp):
        """Process pg_stat_database metrics and calculate commits per second."""
        (numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
         tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
         conflicts, temp_files, temp_bytes, deadlocks) = data

        # Calculate commits per second
        commits_per_second = self._calculate_commits_per_second(xact_commit, current_timestamp)

        # Update for next iteration
        self.previous_xact_commit = xact_commit
        self.previous_timestamp = current_timestamp

        cache_hit_ratio = self._calculate_cache_hit_ratio(blks_read, blks_hit)

        return {
            'active_connections': numbackends,
            'xact_commit': xact_commit,
            'xact_rollback': xact_rollback,
            'blks_read': blks_read,
            'blks_hit': blks_hit,
            'cache_hit_ratio': cache_hit_ratio,
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

    def _process_pg_stat_user_tables(self, user_tables_data):
        """Process pg_stat_user_tables metrics."""
        user_tables_stats = []
        for row in user_tables_data:
            (relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd,
             n_tup_del, n_live_tup, n_dead_tup, vacuum_count, autovacuum_count) = row

            user_tables_stats.append({
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
            })

        return user_tables_stats

    def _calculate_commits_per_second(self, xact_commit, current_timestamp):
        """Calculate the commits per second based on the current and previous values."""
        if self.previous_xact_commit is not None and self.previous_timestamp is not None:
            time_diff = current_timestamp - self.previous_timestamp
            if time_diff > 0:
                commits_per_second = (xact_commit - self.previous_xact_commit) / time_diff
                logger.info(f"Calculated commits per second: {commits_per_second:.2f}")
                return commits_per_second
        logger.info("No previous commit data available to calculate commits per second")
        return 0

    def _calculate_cache_hit_ratio(self, blks_read, blks_hit):
        """Calculate the cache hit ratio."""
        if blks_read + blks_hit == 0:
            return 1.0  # If no reads, assume a perfect cache hit ratio
        return blks_hit / (blks_read + blks_hit)
