import time

from dashboard.data.mongo_data import MongoDataFetcher
from dashboard.logger.logging_config import logger


class MongoMetrics:
    def __init__(self):
        self.fetcher = MongoDataFetcher()

    def get_metrics(self):
        logger.info("Fetching MongoDB performance metrics...")
        current_timestamp = time.time()

        try:
            # Fetch different MongoDB metrics
            server_status = self.fetcher.fetch_server_status()
            db_stats = self.fetcher.fetch_db_stats()
            current_operations = self.fetcher.fetch_current_operations()
            top_metrics = self.fetcher.fetch_top_metrics()
            repl_status = self.fetcher.fetch_repl_status()
            shard_status = self.fetcher.fetch_shard_status()

            metrics = {'timestamp': current_timestamp}

            # Process server status
            if server_status:
                logger.info("Processing server status metrics")
                metrics['server_status'] = self._process_server_status(server_status)
            else:
                logger.error("Failed to fetch server status metrics")

            # Process database statistics
            if db_stats:
                logger.info("Processing database statistics")
                metrics['db_stats'] = self._process_db_stats(db_stats)
            else:
                logger.error("Failed to fetch database statistics")

            # Process current operations
            if current_operations:
                logger.info("Processing current operations")
                metrics['current_operations'] = current_operations
            else:
                logger.error("Failed to fetch current operations")

            # Process top metrics
            if top_metrics:
                logger.info("Processing top metrics")
                metrics['top_metrics'] = self._process_top_metrics(top_metrics)
            else:
                logger.error("Failed to fetch top metrics")

            # Process replication status
            if repl_status:
                logger.info("Processing replication status")
                metrics['replication_status'] = repl_status
            else:
                logger.warning("No replication status available")

            # Process shard status
            if shard_status:
                logger.info("Processing shard status")
                metrics['shard_status'] = shard_status
            else:
                logger.warning("No shard status available")

            logger.info("Successfully fetched all MongoDB metrics")
            return metrics

        except Exception as e:
            logger.error(f"Error fetching MongoDB metrics: {e}")
            return {'error': str(e)}

    def _process_server_status(self, server_status):
        """Extract relevant server status metrics."""
        return {
            'uptime': server_status.get('uptime'),
            'connections': server_status.get('connections', {}),
            'opcounters': server_status.get('opcounters', {}),
            'network': server_status.get('network', {}),
            'mem': server_status.get('mem', {}),
            'wiredTiger': server_status.get('wiredTiger', {}),
            'extra_info': server_status.get('extra_info', {})
        }

    def _process_db_stats(self, db_stats):
        """Extract relevant database statistics."""
        return {
            'db_name': db_stats.get('db'),
            'collections': db_stats.get('collections'),
            'objects': db_stats.get('objects'),
            'avg_obj_size': db_stats.get('avgObjSize'),
            'data_size': db_stats.get('dataSize'),
            'storage_size': db_stats.get('storageSize'),
            'indexes': db_stats.get('indexes'),
            'index_size': db_stats.get('indexSize')
        }

    def _process_top_metrics(self, top_metrics):
        """Extract per-collection operation counters."""
        totals = top_metrics.get('totals', {})
        processed_top_metrics = {}

        for collection, stats in totals.items():
            processed_top_metrics[collection] = {
                'total_time': stats.get('total', {}).get('time', 0),
                'read_count': stats.get('readLock', {}).get('count', 0),
                'write_count': stats.get('writeLock', {}).get('count', 0),
            }

        return processed_top_metrics

    def _process_replication_status(self, repl_status):
        """Extract relevant replication status metrics."""
        members = repl_status.get('members', [])
        replication_metrics = []

        for member in members:
            replication_metrics.append({
                'name': member.get('name'),
                'stateStr': member.get('stateStr'),
                'uptime': member.get('uptime'),
                'optimeDate': member.get('optimeDate'),
                'lag': member.get('lag', 0)
            })

        return replication_metrics

    def _process_shard_status(self, shard_status):
        """Extract relevant shard status metrics."""
        shards = shard_status.get('shards', {})
        processed_shards = {}

        for shard_name, shard_info in shards.items():
            processed_shards[shard_name] = {
                'host': shard_info.get('host'),
                'state': shard_info.get('state')
            }

        return processed_shards
