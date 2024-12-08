from dashboard.data.postgres_metrics import PostgresMetrics
from postgres_scraper.metrics import get_pg_metrics

metrics_fetcher = PostgresMetrics()
data = metrics_fetcher.get_metrics()
baseline_data = None

# Set baseline if not already set
if not baseline_data:
    baseline_data = data

# Append current metrics to historical data
historical_data.append(data)
if len(historical_data) > 300:
    historical_data = historical_data[-300:]

# Calculate relative metrics
relative_metrics = {
    key: data[key] - baseline_data[key]
    for key in ['xact_commit', 'xact_rollback', 'blks_read', 'blks_hit',
                'tup_returned', 'tup_fetched', 'tup_inserted', 'tup_updated', 'tup_deleted']
}

data2 = get_pg_metrics()
a = 1
