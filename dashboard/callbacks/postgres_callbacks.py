import traceback

import plotly.graph_objs as go
from dashboard.app import app
from dash import Output, Input, State, dcc

from dashboard.figures.postgres_figures import PostgresFigures
from dashboard.logger.logging_config import logger
from dashboard.collector.metrics_file_handler import MetricsFileHandler

metrics_file_handler = MetricsFileHandler("dashboard//collector//postgres_metrics.json")


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
        Output("pg-cache-hit-ratio-line", "figure"),
        Output("pg-checkpoints-over-time", "figure"),
        Output("pg-buffer-writes-stacked", "figure"),
        Output("pg-index-usage-bar", "figure"),
        Output("pg-index-usage-combined", "figure"),
        Output("pg-insert-fig", "figure"),
        Output("pg-update-fig", "figure"),
        Output("pg-delete-fig", "figure"),
        Output("pg-fetched-fig", "figure"),
        Output("pg-returns-fig", "figure"),
    ],
    [Input("pg-interval", "n_intervals")],
    [State('historical-data', 'data'), State('baseline-data', 'data')]
)
def update_postgres_figures(n_intervals, historical_data, baseline_data):
    logger.info(f"Callback triggered: update_postgres_figures (n_intervals={n_intervals})")

    historical_data = historical_data if historical_data is not None else []
    baseline_data = baseline_data if baseline_data is not None else None
    try:
        data = metrics_file_handler.read_metrics_from_file()

        if not data:
            error_message = "Error: No Postgres metrics available. Data fetch failed or is delayed."
            logger.error(error_message)
            raise Exception(error_message)

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

        checkpoints_over_time_fig = PostgresFigures.checkpoints_over_time_chart(historical_data)
        buffer_writes_stacked_fig = PostgresFigures.buffer_writes_stacked_chart(data)

        index_usage_bar_fig = PostgresFigures.index_usage_bar_chart(data['index_usage'])
        index_usage_combined_fig = PostgresFigures.index_usage_combined_chart(data['index_usage'])

        insert_fig = PostgresFigures.cumulative_and_rate_graph(historical_data, 'tup_inserted', 'Insert Operations')
        update_fig = PostgresFigures.cumulative_and_rate_graph(historical_data, 'tup_updated', 'Update Operations')
        delete_fig = PostgresFigures.cumulative_and_rate_graph(historical_data, 'tup_deleted', 'Delete Operations')
        fetched_fig = PostgresFigures.cumulative_and_rate_graph(historical_data, 'tup_fetched', 'Fetched Tuples')
        returns_fig = PostgresFigures.cumulative_and_rate_graph(historical_data, 'tup_returned', 'Returned Tuples')

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
            cache_hit_ratio_line_fig,
            checkpoints_over_time_fig,
            buffer_writes_stacked_fig,
            index_usage_bar_fig,
            index_usage_combined_fig,
            insert_fig,
            update_fig,
            delete_fig,
            fetched_fig,
            returns_fig
        ]

    except Exception as e:
        error_type = type(e).__name__  # Get the exception type
        tb = traceback.format_exc()  # Get the full traceback as a string
        logger.error(f"Error updating figures: {error_type} - {e}\nTraceback:\n{tb}")
        empty_fig = go.Figure()
        return [
            historical_data or [],  # historical-data
            baseline_data or None,  # baseline-data
            empty_fig,  # pg-active-connections
            empty_fig,  # pg-transactions
            empty_fig,  # pg-blocks
            empty_fig,  # pg-tuples
            empty_fig,  # pg-conflicts-deadlocks
            empty_fig,  # pg-temp-usage
            empty_fig,  # pg-commits-over-time
            empty_fig,  # pg-commits-per-second
            empty_fig,  # pg-user-tables-stats
            [],  # pg-stat-activity-rows (children)
            empty_fig,  # pg-cache-hit-ratio-gauge
            empty_fig,  # pg-cache-hit-ratio-line
            empty_fig,  # pg-checkpoints-over-time
            empty_fig,  # pg-buffer-writes-stacked
            empty_fig,  # pg-index-usage-bar
            empty_fig,  # pg-index-usage-combined
            empty_fig,  # pg-insert-fig
            empty_fig,  # pg-update-fig
            empty_fig,  # pg-delete-fig
            empty_fig,  # pg-fetched-fig
            empty_fig  # pg-returns-fig
        ]
