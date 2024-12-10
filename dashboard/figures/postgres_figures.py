import ast
import time
from datetime import datetime

import plotly.graph_objs as go


class PostgresFigures:
    @staticmethod
    def active_connections_gauge(value):
        return go.Figure(go.Indicator(
            mode="gauge+number",
            value=value,
            title={'text': "Active Connections"},
            gauge={'axis': {'range': [0, 100]}}
        ))

    @staticmethod
    def transactions_graph(commit, rollback):
        return go.Figure(
            data=[
                go.Bar(name='Committed', x=['Transactions'], y=[commit]),
                go.Bar(name='Rolled Back', x=['Transactions'], y=[rollback])
            ],
            layout=go.Layout(
                title='Transactions (Committed vs Rolled Back)',
                barmode='group'
            )
        )

    @staticmethod
    def blocks_graph(read, hit):
        return go.Figure(
            data=[
                go.Bar(name='Blocks Read', x=['Blocks'], y=[read]),
                go.Bar(name='Blocks Hit', x=['Blocks'], y=[hit])
            ],
            layout=go.Layout(
                title='Blocks Read vs Blocks Hit',
                barmode='group'
            )
        )

    @staticmethod
    def tuples_graph(tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted):
        return go.Figure(
            data=[
                go.Bar(name='Returned', x=['Tuples'], y=[tup_returned]),
                go.Bar(name='Fetched', x=['Tuples'], y=[tup_fetched]),
                go.Bar(name='Inserted', x=['Tuples'], y=[tup_inserted]),
                go.Bar(name='Updated', x=['Tuples'], y=[tup_updated]),
                go.Bar(name='Deleted', x=['Tuples'], y=[tup_deleted])
            ],
            layout=go.Layout(
                title='Tuples Metrics',
                barmode='group'
            )
        )

    @staticmethod
    def conflicts_deadlocks_graph(conflicts, deadlocks):
        return go.Figure(
            data=[
                go.Bar(name='Conflicts', x=['Events'], y=[conflicts]),
                go.Bar(name='Deadlocks', x=['Events'], y=[deadlocks])
            ],
            layout=go.Layout(
                title='Conflicts & Deadlocks',
                barmode='group'
            )
        )

    @staticmethod
    def temp_usage_graph(temp_files, temp_bytes):
        return go.Figure(
            data=[
                go.Bar(name='Temp Files', x=['Temp Usage'], y=[temp_files]),
                go.Bar(name='Temp Bytes (MB)', x=['Temp Usage'], y=[temp_bytes / 1e6])
            ],
            layout=go.Layout(
                title='Temporary Files and Bytes (MB)',
                barmode='group'
            )
        )

    @staticmethod
    def user_table_stats_graph(user_tables_stats):
        table_names = [t['table_name'] for t in user_tables_stats]
        tup_returned = [t['seq_tup_read'] for t in user_tables_stats]
        tup_fetched = [t['idx_tup_fetch'] for t in user_tables_stats]

        return go.Figure(
            data=[
                go.Bar(name='Rows Returned (seq_tup_read)', x=table_names, y=tup_returned),
                go.Bar(name='Rows Fetched (idx_tup_fetch)', x=table_names, y=tup_fetched)
            ],
            layout=go.Layout(
                title='Rows Returned vs Fetched per Table',
                xaxis_title='Table Name',
                yaxis_title='Number of Rows',
                barmode='group'
            )
        )

    @staticmethod
    def pg_stat_activity_table(pg_stat_activity, max_rows=10):
        headers = ["PID", "User", "State", "Query", "Backend Start", "State Change"]
        rows = []

        for entry in pg_stat_activity:
            try:
                # Parse the string entry into a tuple using ast.literal_eval
                parsed_entry = ast.literal_eval(entry)

                # Ensure parsed_entry has the expected structure
                pid = str(parsed_entry[0])
                user = parsed_entry[1] if parsed_entry[1] else 'N/A'
                state = parsed_entry[2] if parsed_entry[2] else 'N/A'
                query = parsed_entry[3][:100] if parsed_entry[3] else 'N/A'
                backend_start = parsed_entry[4].strftime('%Y-%m-%d %H:%M:%S') if parsed_entry[4] else 'N/A'
                state_change = parsed_entry[5].strftime('%Y-%m-%d %H:%M:%S') if parsed_entry[5] else 'N/A'

                # Append the processed row
                rows.append([pid, user, state, query, backend_start, state_change])

            except Exception as e:
                # Handle parsing errors gracefully
                rows.append(['Error', 'Error', 'Error', f'Failed to parse: {str(e)}', 'N/A', 'N/A'])

        table_chunks = [rows[i:i + max_rows] for i in range(0, len(rows), max_rows)]
        figures = []

        for i, chunk in enumerate(table_chunks):
            fig = go.Figure(
                data=[go.Table(
                    header=dict(values=headers, align='left'),
                    cells=dict(values=[list(col) for col in zip(*chunk)], align='left')
                )],
                layout=go.Layout(title=f'Active Connections and Queries (Page {i + 1})')
            )
            figures.append(fig)

        return figures

    @staticmethod
    def build_time_series_line_chart(historical_metrics, metric_key, title):
        # Extract timestamps and values
        timestamps = [datetime.fromtimestamp(m['timestamp']).strftime('%Y-%m-%d %H:%M:%S') for m in historical_metrics]
        values = [m[metric_key] for m in historical_metrics]

        return go.Figure(
            data=[go.Scatter(x=timestamps, y=values, mode='lines+markers')],
            layout=go.Layout(
                title=title,
                xaxis_title="Time",
                yaxis_title=metric_key,
                xaxis=dict(
                    tickangle=45  # Rotate x-axis labels for better readability
                )
            )
        )

    @staticmethod
    def build_commits_per_second_chart(historical_metrics):
        """Build a time-series chart for commits per second."""
        timestamps = [time.strftime('%H:%M:%S', time.localtime(m['timestamp'])) for m in historical_metrics]
        commits_per_second = [m['commits_per_second'] for m in historical_metrics]

        return go.Figure(
            data=[go.Scatter(x=timestamps, y=commits_per_second, mode='lines+markers', name='Commits/sec')],
            layout=go.Layout(
                title='Transactions Committed per Second',
                xaxis_title='Time',
                yaxis_title='Commits/sec'
            )
        )

    @staticmethod
    def cache_hit_ratio_gauge(current_ratio):
        """Create an enhanced gauge for the current cache hit ratio."""
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=current_ratio * 100,
            delta={'reference': 95, 'position': "bottom", 'relative': False},
            title={"text": "Cache Hit Ratio (%)", "font": {"size": 24}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 2, "tickcolor": "darkblue"},
                "bar": {"color": "darkblue"},
                "steps": [
                    {"range": [0, 80], "color": "red"},
                    {"range": [80, 95], "color": "orange"},
                    {"range": [95, 100], "color": "green"}
                ],
                "threshold": {
                    "line": {"color": "black", "width": 4},
                    "thickness": 0.75,
                    "value": 95
                }
            }
        ))

        return fig

    @staticmethod
    def cache_hit_ratio_over_time_chart(historical_data):
        """Create an enhanced line chart showing cache hit ratio over time."""
        # Convert timestamps to readable datetime strings
        timestamps = [datetime.fromtimestamp(entry['timestamp']).strftime('%H:%M:%S') for entry in
                      historical_data]
        ratios = [entry['cache_hit_ratio'] * 100 for entry in historical_data]

        return go.Figure(
            data=go.Scatter(
                x=timestamps,
                y=ratios,
                mode='lines+markers',
                line=dict(width=3),
                marker=dict(size=6, color='blue'),
                name='Cache Hit Ratio'
            ),
            layout=go.Layout(
                title={
                    'text': "Cache Hit Ratio Over Time",
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 24}
                },
                xaxis={
                    "title": "Time",
                    "tickangle": 45,
                    "showgrid": True,
                    "gridcolor": 'lightgrey',
                    "tickformat": "%Y-%m-%d %H:%M:%S"  # Ensure proper datetime format
                },
                yaxis={
                    "title": "Cache Hit Ratio (%)",
                    "range": [0, 100],
                    "showgrid": True,
                    "gridcolor": 'lightgrey'
                },
                margin=dict(l=40, r=40, t=50, b=80),
                height=400
            )
        )

    @staticmethod
    def checkpoints_over_time_chart(historical_data):
        """Create a time series line chart for checkpoints over time."""
        timestamps = [datetime.fromtimestamp(entry['timestamp']).strftime('%Y-%m-%d %H:%M:%S') for entry in
                      historical_data]
        checkpoints_timed = [entry['bgwriter']['checkpoints_timed'] for entry in historical_data]
        checkpoints_req = [entry['bgwriter']['checkpoints_req'] for entry in historical_data]

        return go.Figure(
            data=[
                go.Scatter(x=timestamps, y=checkpoints_timed, mode='lines+markers', name='Checkpoints Timed'),
                go.Scatter(x=timestamps, y=checkpoints_req, mode='lines+markers', name='Checkpoints Requested')
            ],
            layout=go.Layout(
                title="Checkpoints Over Time",
                xaxis={"title": "Time", "tickangle": 45},
                yaxis={"title": "Checkpoints"},
                margin=dict(l=40, r=40, t=50, b=80),
                height=400
            )
        )

    @staticmethod
    def buffer_writes_stacked_chart(data):
        """Create a stacked bar chart for buffer writes."""
        labels = ['Buffers Checkpoint', 'Buffers Clean', 'Buffers Backend', 'Buffers Backend Fsync']
        values = [
            data['bgwriter']['buffers_checkpoint'],
            data['bgwriter']['buffers_clean'],
            data['bgwriter']['buffers_backend'],
            data['bgwriter']['buffers_backend_fsync']
        ]

        return go.Figure(
            data=go.Bar(x=labels, y=values, marker_color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']),
            layout=go.Layout(
                title="Buffer Writes Breakdown",
                xaxis={"title": "Buffer Write Types"},
                yaxis={"title": "Number of Buffers"},
                margin=dict(l=40, r=40, t=50, b=40),
                height=400
            )
        )

    @staticmethod
    def index_usage_bar_chart(index_usage_data):
        """Create a bar chart comparing idx_scan vs seq_scan for each table."""
        tables = [entry['table_name'] for entry in index_usage_data]
        seq_scans = [entry['seq_scan'] for entry in index_usage_data]
        idx_scans = [entry['idx_scan'] for entry in index_usage_data]

        return go.Figure(
            data=[
                go.Bar(name='Sequential Scans', x=tables, y=seq_scans, marker_color='orange'),
                go.Bar(name='Index Scans', x=tables, y=idx_scans, marker_color='blue')
            ],
            layout=go.Layout(
                title="Index vs Sequential Scans per Table",
                xaxis={"title": "Tables", "tickangle": 45},
                yaxis={"title": "Number of Scans"},
                barmode='group',
                height=500
            )
        )

    @staticmethod
    def index_usage_combined_chart(index_usage_data):
        """Create a combined chart for index vs sequential scans with tuples returned."""
        tables = [entry['table_name'] for entry in index_usage_data]
        seq_tup_read = [entry['seq_tup_read'] for entry in index_usage_data]
        idx_tup_fetch = [entry['idx_tup_fetch'] for entry in index_usage_data]

        return go.Figure(
            data=[
                go.Bar(name='Sequential Tuples Read', x=tables, y=seq_tup_read, marker_color='red'),
                go.Bar(name='Index Tuples Fetched', x=tables, y=idx_tup_fetch, marker_color='green')
            ],
            layout=go.Layout(
                title="Tuples Returned by Sequential and Index Scans",
                xaxis={"title": "Tables", "tickangle": 45},
                yaxis={"title": "Number of Tuples"},
                barmode='group',
                height=500
            )
        )

    @staticmethod
    def cumulative_and_rate_graph(historical_data, metric_key, title):
        """
        Generates a combined cumulative and rate line chart for a given metric.

        Parameters:
        - historical_data: List of historical metrics.
        - metric_key: Key for the metric to visualize (e.g., 'tup_inserted').
        - title: Title of the chart.

        Returns:
        - A Plotly Figure object.
        """
        if not historical_data:
            return go.Figure()

        # Extract timestamps and convert to datetime objects
        timestamps = [datetime.utcfromtimestamp(entry['timestamp']) for entry in historical_data]
        cumulative_values = [entry[metric_key] for entry in historical_data]

        # Calculate rates (difference between consecutive cumulative values)
        rates = [0] + [
            (cumulative_values[i] - cumulative_values[i - 1]) / (timestamps[i] - timestamps[i - 1]).total_seconds()
            for i in range(1, len(cumulative_values))
        ]

        # Create figure
        fig = go.Figure()

        # Add cumulative line
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=cumulative_values,
            mode='lines+markers',
            name='Cumulative'
        ))

        # Add rate line
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=rates,
            mode='lines+markers',
            name='Rate (per second)',
            yaxis='y2'
        ))

        # Update layout to have two y-axes
        fig.update_layout(
            title=title,
            xaxis_title='Time',
            yaxis=dict(title='Cumulative Count'),
            yaxis2=dict(
                title='Rate (per second)',
                overlaying='y',
                side='right'
            ),
            legend=dict(x=0, y=1),
            hovermode='x unified',
            xaxis=dict(
                tickformat='%H:%M:%S',
                tickangle=45
            ),
            margin=dict(b=100)  # Ensure space for angled labels
        )

        return fig
