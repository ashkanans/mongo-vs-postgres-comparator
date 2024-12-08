import plotly.graph_objs as go
from app import app
from dash import Output, Input, State, dcc

from dashboard.data.postgres_metrics import PostgresMetrics
from dashboard.figures.postgres_figures import PostgresFigures
from dashboard.logger.logging_config import logger

metrics_fetcher = PostgresMetrics()


@app.callback(
    [
        Output('historical-data', 'data'),
        Output('baseline-data', 'data'),
        Output("pg-active-connections", "figure"),
        Output("pg-transactions", "figure"),
        Output("pg-blocks", "figure"),
        Output("pg-tuples", "figure"),
        Output("pg-conflicts-deadlocks", "figure"),
        Output("pg-temp-usage", "figure"),
        Output('pg-commits-over-time', 'figure'),
        Output('pg-commits-per-second', 'figure'),
        Output("pg-user-tables-stats", "figure"),
        Output("pg-stat-activity-rows", "children"),
        Output("pg-cache-hit-ratio-gauge", "figure"),
        Output("pg-cache-hit-ratio-line", "figure")
    ],
    Input("pg-interval", "n_intervals"),
    [State('historical-data', 'data'), State('baseline-data', 'data')]

)
def update_postgres_figures(n_intervals, historical_data, baseline_data):
    logger.info(f"Callback triggered: update_postgres_figures (n_intervals={n_intervals})")
    try:
        data = metrics_fetcher.get_metrics()

        if data is None or 'error' in data:
            logger.error("Error fetching metrics: Returning empty figures")
            empty_fig = go.Figure()
            return [empty_fig] * 8

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

        # Generate figures
        active_conn_fig = PostgresFigures.active_connections_gauge(data['active_connections'])
        transactions_fig = PostgresFigures.transactions_graph(relative_metrics['xact_commit'],
                                                              relative_metrics['xact_rollback'])
        blocks_fig = PostgresFigures.blocks_graph(relative_metrics['blks_read'], relative_metrics['blks_hit'])
        tuples_fig = PostgresFigures.tuples_graph(relative_metrics['tup_returned'], relative_metrics['tup_fetched'],
                                                  relative_metrics['tup_inserted'], relative_metrics['tup_updated'],
                                                  relative_metrics['tup_deleted'])
        conflicts_deadlocks_fig = PostgresFigures.conflicts_deadlocks_graph(data['conflicts'], data['deadlocks'])
        temp_usage_fig = PostgresFigures.temp_usage_graph(data['temp_files'], data['temp_bytes'])
        user_tables_stats_fig = PostgresFigures.user_table_stats_graph(data.get('user_tables_stats', []))

        commits_over_time_fig = PostgresFigures.build_time_series_line_chart(historical_data, 'xact_commit',
                                                                             'Transaction Commits Over Time')
        commits_per_second_fig = PostgresFigures.build_commits_per_second_chart(historical_data)

        activity_figs = PostgresFigures.pg_stat_activity_table(data['pg_stat_activity'])
        activity_rows = [dcc.Graph(figure=fig) for fig in activity_figs]

        cache_hit_ratio_gauge_fig = PostgresFigures.cache_hit_ratio_gauge(data['cache_hit_ratio'])
        cache_hit_ratio_line_fig = PostgresFigures.cache_hit_ratio_over_time_chart(historical_data)

        logger.info("Figures updated successfully")
        return [
            historical_data,
            baseline_data,
            active_conn_fig,
            transactions_fig,
            blocks_fig,
            tuples_fig,
            conflicts_deadlocks_fig,
            temp_usage_fig,
            commits_over_time_fig,
            commits_per_second_fig,
            user_tables_stats_fig,
            activity_rows,
            cache_hit_ratio_gauge_fig,
            cache_hit_ratio_line_fig
        ]

    except Exception as e:
        logger.error(f"Error updating figures: {e}")
        empty_fig = go.Figure()
        return [empty_fig] * 8
