import plotly.graph_objs as go
from app import app
from dash import Output, Input, State, html

from dashboard.data.mongo_metric import MongoMetrics
from dashboard.figures.mongo_figures import MongoFigures
from dashboard.logger.logging_config import logger

mongo_metrics_fetcher = MongoMetrics()


def make_json_serializable(data):
    """
    Recursively convert non-serializable data types to serializable types.
    """
    if isinstance(data, dict):
        return {k: make_json_serializable(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [make_json_serializable(item) for item in data]
    elif isinstance(data, float):
        # Ensure floats are JSON-serializable, e.g., by limiting precision
        return round(data, 6)
    elif isinstance(data, (int, str, bool)) or data is None:
        return data
    else:
        # Convert any other types (like Timestamp) to string
        return str(data)


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
        Output("mongo-cache-over-time", "figure")
    ],
    Input("mongo-interval", "n_intervals"),
    [State('historical-mongo-data', 'data'), State('baseline-mongo-data', 'data')]
)
def update_mongo_figures(n_intervals, historical_data, baseline_data):
    logger.info(f"Callback triggered: update_mongo_figures (n_intervals={n_intervals})")

    try:
        data = mongo_metrics_fetcher.get_metrics()

        # Ensure data is serializable
        data = make_json_serializable(data)

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
            cache_over_time_fig
        )

    except Exception as e:
        logger.error(f"Error updating MongoDB figures: {e}")
        empty_fig = go.Figure()
        return (
            historical_data or [],
            baseline_data or None,
            empty_fig,
            empty_fig,
            empty_fig,
            empty_fig,
            empty_fig,
            empty_fig,
            html.Div("No current operations"),
            empty_fig,
            empty_fig
        )
