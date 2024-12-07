# app.py
from dash import Dash, dcc, html
from dash.dependencies import Output, Input, State

from postgres_scraper.figures import build_active_connections_gauge, build_blocks_graph, build_tuples_graph, \
    build_conflicts_deadlocks_graph, build_temp_usage_graph, build_transactions_graph, build_time_series_line_chart
from postgres_scraper.metrics import get_pg_metrics

# Initialize the Dash app
app = Dash(__name__)
app.title = "PostgreSQL Metrics Dashboard"

app.layout = html.Div([
    html.H1("PostgreSQL Metrics Dashboard", style={'textAlign': 'center', 'marginBottom': '30px'}),

    # Hidden store to keep track of historical data
    dcc.Store(id='historical-data', storage_type='memory', data=[]),

    # Row 1
    html.Div([
        html.Div([dcc.Graph(id='active-connections-gauge')], style={'width': '24%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='transactions-graph')],
                 style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'}),
        html.Div([dcc.Graph(id='blocks-graph')], style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'}),
        html.Div([dcc.Graph(id='tuples-graph')], style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 2
    html.Div([
        html.Div([dcc.Graph(id='conflicts-deadlocks-graph')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='temp-usage-graph')],
                 style={'width': '49%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 3: Time-series charts to show changes over time
    html.Div([
        dcc.Graph(id='commits-over-time')
    ], style={'width': '100%', 'display': 'inline-block'}),

    # Auto-refresh every 2 seconds
    dcc.Interval(
        id='interval-component',
        interval=2 * 1000,  # 2 seconds
        n_intervals=0
    )
])


@app.callback(
    [Output('historical-data', 'data')],
    [Input('interval-component', 'n_intervals')],
    [State('historical-data', 'data')]
)
def update_historical_data(n_intervals, historical_data):
    metrics = get_pg_metrics()
    if metrics is None:
        return [historical_data]

    # Append current metrics to historical data
    historical_data.append(metrics)
    # Limit stored data size if needed
    if len(historical_data) > 300:
        historical_data = historical_data[-300:]
    return [historical_data]


@app.callback(
    [
        Output('active-connections-gauge', 'figure'),
        Output('transactions-graph', 'figure'),
        Output('blocks-graph', 'figure'),
        Output('tuples-graph', 'figure'),
        Output('conflicts-deadlocks-graph', 'figure'),
        Output('temp-usage-graph', 'figure'),
        Output('commits-over-time', 'figure')
    ],
    [Input('historical-data', 'data')]
)
def update_charts(historical_data):
    if not historical_data:
        return [{}, {}, {}, {}, {}, {}, {}]
    latest = historical_data[-1]

    active_connections_fig = build_active_connections_gauge(latest['active_connections'])
    transactions_fig = build_transactions_graph(latest['xact_commit'], latest['xact_rollback'])
    blocks_fig = build_blocks_graph(latest['blks_read'], latest['blks_hit'])
    tuples_fig = build_tuples_graph(latest)
    cd_fig = build_conflicts_deadlocks_graph(latest['conflicts'], latest['deadlocks'])
    temp_usage_fig = build_temp_usage_graph(latest['temp_files'], latest['temp_bytes'])

    # Example time-series chart for commits over time
    commits_over_time_fig = build_time_series_line_chart(historical_data, 'xact_commit',
                                                         'Transaction Commits Over Time')

    return (active_connections_fig,
            transactions_fig,
            blocks_fig,
            tuples_fig,
            cd_fig,
            temp_usage_fig,
            commits_over_time_fig)


if __name__ == '__main__':
    app.run_server(debug=True)
