# figures.py
import time
from datetime import datetime

import plotly.graph_objs as go


def build_active_connections_gauge(current_value):
    return go.Figure(go.Indicator(
        mode="gauge+number",
        value=current_value,
        title={'text': "Active Connections"},
        gauge={'axis': {'range': [0, 100]}}
    ))


def build_transactions_graph(commit, rollback):
    return {
        'data': [
            go.Bar(name='Committed', x=['Transactions'], y=[commit]),
            go.Bar(name='Rolled Back', x=['Transactions'], y=[rollback])
        ],
        'layout': go.Layout(
            title='Transactions (Committed vs Rolled Back)',
            barmode='group'
        )
    }


def build_blocks_graph(read, hit):
    return {
        'data': [
            go.Bar(name='Blocks Read', x=['Blocks'], y=[read]),
            go.Bar(name='Blocks Hit', x=['Blocks'], y=[hit])
        ],
        'layout': go.Layout(
            title='Blocks Read vs Blocks Hit',
            barmode='group'
        )
    }


def build_tuples_graph(metrics):
    return {
        'data': [
            go.Bar(name='Returned', x=['Tuples'], y=[metrics['tup_returned']]),
            go.Bar(name='Fetched', x=['Tuples'], y=[metrics['tup_fetched']]),
            go.Bar(name='Inserted', x=['Tuples'], y=[metrics['tup_inserted']]),
            go.Bar(name='Updated', x=['Tuples'], y=[metrics['tup_updated']]),
            go.Bar(name='Deleted', x=['Tuples'], y=[metrics['tup_deleted']])
        ],
        'layout': go.Layout(
            title='Tuples Metrics',
            barmode='group'
        )
    }


def build_conflicts_deadlocks_graph(conflicts, deadlocks):
    return {
        'data': [
            go.Bar(name='Conflicts', x=['Events'], y=[conflicts]),
            go.Bar(name='Deadlocks', x=['Events'], y=[deadlocks])
        ],
        'layout': go.Layout(
            title='Conflicts & Deadlocks',
            barmode='group'
        )
    }


def build_temp_usage_graph(temp_files, temp_bytes):
    # Display temp usage. Could show temp_bytes in MB.
    return {
        'data': [
            go.Bar(name='Temp Files', x=['Temp Usage'], y=[temp_files]),
            go.Bar(name='Temp Bytes', x=['Temp Usage'], y=[temp_bytes / 1e6], text=["MB"], textposition='auto')
        ],
        'layout': go.Layout(
            title='Temporary Files and Bytes (MB)',
            barmode='group'
        )
    }


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


def build_user_table_stats_graph(user_tables_stats):
    """Build a bar chart for tup_returned and tup_fetched per table."""
    table_names = [table['table_name'] for table in user_tables_stats]
    tup_returned = [table['seq_tup_read'] for table in user_tables_stats]
    tup_fetched = [table['idx_tup_fetch'] for table in user_tables_stats]

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


def build_pg_stat_activity_table(pg_stat_activity, max_rows=10):
    """Build a table to display pg_stat_activity metrics, split into chunks if needed."""
    headers = ["PID", "User", "State", "Query", "Backend Start", "State Change"]
    rows = [
        [
            str(row[0]),  # pid
            row[1] if row[1] else 'N/A',  # usename
            row[2] if row[2] else 'N/A',  # state
            row[3][:100] if row[3] else 'N/A',  # query (truncated to 100 characters)
            row[4].strftime('%Y-%m-%d %H:%M:%S') if row[4] else 'N/A',  # backend_start
            row[5].strftime('%Y-%m-%d %H:%M:%S') if row[5] else 'N/A'  # state_change
        ]
        for row in pg_stat_activity
    ]

    # Split the rows into chunks of `max_rows`
    table_chunks = [rows[i:i + max_rows] for i in range(0, len(rows), max_rows)]

    figures = []
    for i, chunk in enumerate(table_chunks):
        fig = go.Figure(
            data=[go.Table(
                header=dict(values=headers, fill_color='paleturquoise', align='left'),
                cells=dict(values=[list(col) for col in zip(*chunk)], fill_color='lavender', align='left')
            )],
            layout=go.Layout(title=f'Active Connections and Queries (Page {i + 1})')
        )
        figures.append(fig)

    return figures
