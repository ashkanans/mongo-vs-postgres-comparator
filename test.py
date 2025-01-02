from dashboard.figures.postgres_figures import PostgresFigures
from dashboard.collector.metrics_file_handler import MetricsFileHandler

metrics_file_handler = MetricsFileHandler("dashboard/collector/postgres_metrics.json")

data = metrics_file_handler.read_metrics_from_file()
activity_figs = PostgresFigures.pg_stat_activity_table(data['pg_stat_activity'])
a = 1
