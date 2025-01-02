import plotly.graph_objs as go
from dashboard.app import app
from dash import Output, Input, State, html

from dashboard.figures.mongo_figures import MongoFigures
from dashboard.logger.logging_config import logger
from dashboard.collector.metrics_file_handler import MetricsFileHandler

metrics_file_handler = MetricsFileHandler("dashboard//collector//mongo_metrics.json")


@app.callback(
    [
        Output('historical-mongo-data', 'data'),
        Output('baseline-mongo-data', 'data'),
        Output("mongo-active-connections", "figure"),
        Output("mongo-opcounters", "figure"),
        Output("mongo-network-usage", "figure"),
        Output("mongo-memory-usage", "figure"),
        Output("mongo-db-size", "figure"),
        Output("mongo-ops-over-time", "figure"),
        Output("mongo-current-operations", "children"),
        Output("mongo-top-metrics", "figure"),
        Output("mongo-cache-over-time", "figure"),
        Output("mongo-insert-fig", "figure"),
        Output("mongo-delete-fig", "figure"),
        Output("mongo-update-fig", "figure"),
        Output("mongo-query-fig", "figure"),
        Output("mongo-getmore-fig", "figure"),
        Output("mongo-command-fig", "figure")
    ],
    [Input("mongo-interval", "n_intervals")],
    [State('historical-mongo-data', 'data'), State('baseline-mongo-data', 'data')]
)
def update_mongo_figures(n_intervals, historical_data, baseline_data):
    logger.info(f"Callback triggered: update_mongo_figures (n_intervals={n_intervals})")

    historical_data = historical_data if historical_data is not None else []
    baseline_data = baseline_data if baseline_data is not None else None
    try:
        data = metrics_file_handler.read_metrics_from_file()

        if not data:
            error_message = "Error: No MongoDB metrics available. Data fetch failed or is delayed."
            logger.error(error_message)
            raise Exception(error_message)

        if data is None or 'error' in data:
            logger.error("Error fetching MongoDB metrics: Returning empty figures")
            empty_fig = go.Figure()
            return (
                historical_data or [],
                baseline_data or None,
                empty_fig,  # mongo-active-connections
                empty_fig,  # mongo-opcounters
                empty_fig,  # mongo-network-usage
                empty_fig,  # mongo-memory-usage
                empty_fig,  # mongo-db-size
                empty_fig,  # mongo-ops-over-time
                html.Div("No current operations"),
                empty_fig,  # mongo-top-metrics
                empty_fig  # mongo-cache-over-time
            )

        if historical_data is None:
            historical_data = []
        if baseline_data is None:
            baseline_data = data

        historical_data.append(data)
        if len(historical_data) > 300:
            historical_data = historical_data[-300:]

        # Extract metrics with fallback to empty dictionaries if None
        server_status = data.get('server_status') or {}
        db_stats = data.get('db_stats') or {}
        current_operations = data.get('current_operations') or {}
        top_metrics = data.get('top_metrics') or {}

        # Compute relative opcounters with fallback to empty dictionary
        opcounters = server_status.get('opcounters') or {}
        baseline_opcounters = baseline_data.get('server_status', {}).get('opcounters') or {}
        relative_opcounters = {
            k: opcounters.get(k, 0) - baseline_opcounters.get(k, 0)
            for k in opcounters.keys()
        }

        # Generate figures using MongoFigures (with safeguards against None)
        active_connections_fig = MongoFigures.active_connections_gauge(server_status.get('connections', {}))
        opcounters_fig = MongoFigures.opcounters_graph(relative_opcounters)
        network_usage_fig = MongoFigures.network_usage_graph(server_status.get('network', {}))
        memory_usage_fig = MongoFigures.memory_usage_graph(server_status.get('mem', {}))
        db_size_fig = MongoFigures.db_size_graph(db_stats)
        ops_over_time_fig = MongoFigures.ops_over_time_line_chart(historical_data, 'opcounters', 'Operations Over Time')

        # Current operations displayed as a table or text
        current_ops_component = MongoFigures.current_operations_table(current_operations)
        if current_ops_component is None:
            current_ops_component = html.Div("No current operations")

        top_metrics_fig = MongoFigures.top_metrics_chart(top_metrics)
        cache_over_time_fig = MongoFigures.cache_hit_ratio_over_time(historical_data)

        operations = ['insert', 'delete', 'update', 'query', 'getmore', 'command']
        figures = [MongoFigures.cumulative_and_rate_graph(historical_data, op) for op in operations]

        logger.info("MongoDB figures updated successfully")
        return (
            historical_data,
            baseline_data,
            active_connections_fig,
            opcounters_fig,
            network_usage_fig,
            memory_usage_fig,
            db_size_fig,
            ops_over_time_fig,
            current_ops_component,
            top_metrics_fig,
            cache_over_time_fig,
            *figures
        )

    except Exception as e:

        logger.error(f"Error updating MongoDB figures: {e}")

        empty_fig = go.Figure()

        return (
            historical_data or [],
            baseline_data or None,
            empty_fig,  # mongo-active-connections
            empty_fig,  # mongo-opcounters
            empty_fig,  # mongo-network-usage
            empty_fig,  # mongo-memory-usage
            empty_fig,  # mongo-db-size
            empty_fig,  # mongo-ops-over-time
            html.Div("No current operations"),  # mongo-current-operations
            empty_fig,  # mongo-top-metrics
            empty_fig,  # mongo-cache-over-time
            empty_fig,  # mongo-insert-fig
            empty_fig,  # mongo-delete-fig
            empty_fig,  # mongo-update-fig
            empty_fig,  # mongo-query-fig
            empty_fig,  # mongo-getmore-fig
            empty_fig  # mongo-command-fig
        )
