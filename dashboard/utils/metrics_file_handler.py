import json
import os

from dashboard.logger.logging_config import logger


class MetricsFileHandler:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_metrics_from_file(self):
        """Read the latest PostgreSQL metrics from a JSON file."""
        if not os.path.exists(self.file_path):
            logger.error(f"Metrics file not found: {self.file_path}")
            return None

        try:
            with open(self.file_path, "r") as file:
                data = json.load(file)
                logger.info(f"Metrics loaded from file: {self.file_path}")
                return data
        except Exception as e:
            logger.error(f"Error reading metrics from file: {e}")
            return None
