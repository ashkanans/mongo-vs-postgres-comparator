import threading
import time
import json
from datetime import datetime

from dashboard.data.mongo_metric import MongoMetrics
from dashboard.data.postgres_metrics import PostgresMetrics
from dashboard.logger.logging_config import logger


def make_json_serializable(data):
    """
    Recursively convert non-serializable data types (e.g., tuples, datetime) to serializable types.
    """
    if isinstance(data, dict):
        return {k: make_json_serializable(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [make_json_serializable(item) for item in data]
    elif isinstance(data, tuple):
        # Convert tuple to list
        return [make_json_serializable(item) for item in data]
    elif isinstance(data, datetime):
        # Convert datetime to ISO string
        return data.isoformat()
    elif isinstance(data, float):
        # Ensure floats are JSON-serializable by limiting precision
        return round(data, 6)
    elif isinstance(data, (int, str, bool)) or data is None:
        return data
    else:
        # Fallback to string conversion for any other types
        return str(data)



class MetricsCollector:
    def __init__(self):
        self.latest_mongo_metrics = {}
        self.latest_postgres_metrics = {}
        self.latest_mongo_metrics_lock = threading.Lock()

        # Initialize metric classes
        self.mongo_metrics = MongoMetrics()
        self.postgres_metrics = PostgresMetrics()

        # Database name field
        self.database_name = ""

        # Create and start threads
        self.mongo_thread = threading.Thread(target=self.fetch_mongo_metrics_periodically, daemon=True)
        self.postgres_thread = threading.Thread(target=self.fetch_postgres_metrics_periodically, daemon=True)

    def start(self):
        """Start the metric collection threads."""
        self.mongo_thread.start()
        self.postgres_thread.start()
        logger.info("Metric collection threads started.")
        try:
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt:
            logger.info("Stopping metric collection.")

    def save_metrics_to_file(self, metrics, filename_prefix):
        """Save the metrics to a JSON file with a timestamp and measure write time."""
        filename = f"dashboard//collector//{filename_prefix}.json"

        # Ensure metrics are JSON-serializable
        metrics = make_json_serializable(metrics)

        start_time = time.perf_counter()
        with open(filename, "w") as file:
            json.dump(metrics, file, indent=4)
        end_time = time.perf_counter()

        time_taken = end_time - start_time
        logger.info(f"Metrics saved to {filename} in {time_taken:.4f} seconds")

    def fetch_mongo_metrics_periodically(self):
        """Fetch MongoDB metrics periodically and save to file."""
        self.database_name = "mongo"
        while True:
            with self.latest_mongo_metrics_lock:
                self.latest_mongo_metrics = self.mongo_metrics.get_metrics()
                logger.debug(f"latest_mongo_metrics: {self.latest_mongo_metrics}")
                self.save_metrics_to_file(self.latest_mongo_metrics, "mongo_metrics")
            time.sleep(0.5)

    def fetch_postgres_metrics_periodically(self):
        """Fetch PostgreSQL metrics periodically and save to file."""
        self.database_name = "postgres"
        while True:
            self.latest_postgres_metrics = self.postgres_metrics.get_metrics()
            logger.debug(f"latest_postgres_metrics: {self.latest_postgres_metrics}")
            self.save_metrics_to_file(self.latest_postgres_metrics, "postgres_metrics")
            time.sleep(0.5)
