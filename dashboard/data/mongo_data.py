from pymongo import MongoClient
from pymongo.errors import PyMongoError

from dashboard.logger.logging_config import logger

MONGO_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "database": "benchmark_db"
}


class MongoDataFetcher:
    def __init__(self, config=MONGO_CONFIG):
        self.config = config
        self.client = MongoClient(self.config['host'], self.config['port'])
        self.db = self.client[self.config['database']]

    def _execute_command(self, command):
        try:
            logger.info(f"Executing command: {command}")
            result = self.db.command(command)
            logger.info("Command executed successfully")
            return result
        except PyMongoError as e:
            logger.error(f"Error executing command: {command} - {e}")
            return None

    def fetch_server_status(self):
        """Fetch server status metrics."""
        return self._execute_command('serverStatus')

    def fetch_db_stats(self):
        """Fetch database statistics."""
        return self._execute_command('dbStats')

    def fetch_collection_stats(self, collection_name):
        """Fetch collection statistics."""
        return self.db.command('collStats', collection_name)

    def fetch_current_operations(self):
        """Fetch currently running operations."""
        return self._execute_command('currentOp')

    def fetch_index_stats(self, collection_name):
        """Fetch index statistics for a collection."""
        collection = self.db[collection_name]
        try:
            return list(collection.index_information().items())
        except PyMongoError as e:
            logger.error(f"Error fetching index stats for collection {collection_name}: {e}")
            return None

    def fetch_top_metrics(self):
        """Fetch per-collection operation counters."""
        return self._execute_command('top')

    def fetch_repl_status(self):
        """Fetch replica set status if applicable."""
        try:
            return self.client.admin.command('replSetGetStatus')
        except PyMongoError as e:
            logger.warning(f"Not a replica set or error fetching replica status: {e}")
            return None

    def fetch_shard_status(self):
        """Fetch shard cluster status if applicable."""
        try:
            return self.client.admin.command('shardStatus')
        except PyMongoError as e:
            logger.warning(f"Not a sharded cluster or error fetching shard status: {e}")
            return None
