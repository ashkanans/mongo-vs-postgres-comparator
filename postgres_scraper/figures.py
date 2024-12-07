# figures.py
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
    timestamps = [m['timestamp'] for m in historical_metrics]
    values = [m[metric_key] for m in historical_metrics]

    return go.Figure(
        data=[go.Scatter(x=timestamps, y=values, mode='lines+markers')],
        layout=go.Layout(title=title, xaxis_title="Time", yaxis_title=metric_key)
    )
