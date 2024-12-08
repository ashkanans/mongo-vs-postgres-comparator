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
        rows = [
            [
                str(r[0]),
                r[1] if r[1] else 'N/A',
                r[2] if r[2] else 'N/A',
                r[3][:100] if r[3] else 'N/A',
                r[4].strftime('%Y-%m-%d %H:%M:%S') if r[4] else 'N/A',
                r[5].strftime('%Y-%m-%d %H:%M:%S') if r[5] else 'N/A'
            ]
            for r in pg_stat_activity
        ]

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
