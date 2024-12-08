from dash import Dash, dcc, html

# Initialize the Dash app
app = Dash(__name__)
app.title = "PostgreSQL Metrics Dashboard"

app.layout = html.Div([
    html.H1("PostgreSQL Metrics Dashboard", style={'textAlign': 'center', 'marginBottom': '30px'}),

    # Hidden stores to keep track of historical data and baseline data
    dcc.Store(id='historical-data', storage_type='memory', data=[]),
    dcc.Store(id='baseline-data', storage_type='memory', data=None),

    # Row 1
    html.Div([
        html.Div([dcc.Graph(id='active-connections-gauge')], style={'width': '24%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='transactions-graph')],
                 style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'}),
        html.Div([dcc.Graph(id='blocks-graph')], style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'}),
        html.Div([dcc.Graph(id='tuples-graph')], style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    html.Div(id='pg-stat-activity-rows', style={'marginBottom': '40px'}),

    # Row 2
    html.Div([
        html.Div([dcc.Graph(id='conflicts-deadlocks-graph')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='temp-usage-graph')],
                 style={'width': '49%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 3: Time-series charts to show changes over time and per second
    html.Div([
        html.Div([dcc.Graph(id='pg-commits-over-time')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='pg-commits-per-second')],
                 style={'width': '49%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 4: User Table Stats
    html.Div([
        dcc.Graph(id='user-table-stats-graph')
    ], style={'width': '100%', 'display': 'inline-block', 'marginBottom': '40px'}),

    # Auto-refresh every 1 second
    dcc.Interval(
        id='interval-component',
        interval=1 * 1000,  # 1 second
        n_intervals=0
    )
])

from dash import Output, State, Input, dcc

from live_postgres_metrics import app
from postgres_scraper.figures import *
from postgres_scraper.metrics import get_pg_metrics


@app.callback(
    [
        Output('historical-data', 'data'),
        Output('baseline-data', 'data'),
        Output('active-connections-gauge', 'figure'),
        Output('transactions-graph', 'figure'),
        Output('blocks-graph', 'figure'),
        Output('tuples-graph', 'figure'),
        Output('conflicts-deadlocks-graph', 'figure'),
        Output('temp-usage-graph', 'figure'),
        Output('pg-commits-over-time', 'figure'),
        Output('pg-commits-per-second', 'figure'),
        Output('user-table-stats-graph', 'figure'),
        Output('pg-stat-activity-rows', 'children')
    ],
    [Input('interval-component', 'n_intervals')],
    [State('historical-data', 'data'), State('baseline-data', 'data')]
)
def update_dashboard(n_intervals, historical_data, baseline_data):
    # Fetch current metrics
    metrics = get_pg_metrics()
    if metrics is None:
        return historical_data, baseline_data, {}, {}, {}, {}, {}, {}, {}, {}, {}, []

    # Set baseline if not already set
    if not baseline_data:
        baseline_data = metrics

    # Append current metrics to historical data
    historical_data.append(metrics)
    if len(historical_data) > 300:
        historical_data = historical_data[-300:]

    # Calculate relative metrics
    relative_metrics = {
        key: metrics[key] - baseline_data[key]
        for key in ['xact_commit', 'xact_rollback', 'blks_read', 'blks_hit',
                    'tup_returned', 'tup_fetched', 'tup_inserted', 'tup_updated', 'tup_deleted']
    }

    # Generate figures
    active_connections_fig = build_active_connections_gauge(metrics['active_connections'])
    transactions_fig = build_transactions_graph(relative_metrics['xact_commit'], relative_metrics['xact_rollback'])
    blocks_fig = build_blocks_graph(relative_metrics['blks_read'], relative_metrics['blks_hit'])
    tuples_fig = build_tuples_graph(relative_metrics)
    cd_fig = build_conflicts_deadlocks_graph(metrics['conflicts'], metrics['deadlocks'])
    temp_usage_fig = build_temp_usage_graph(metrics['temp_files'], metrics['temp_bytes'])
    commits_over_time_fig = build_time_series_line_chart(historical_data, 'xact_commit',
                                                         'Transaction Commits Over Time')
    commits_per_second_fig = build_commits_per_second_chart(historical_data)

    user_table_stats_fig = build_user_table_stats_graph(metrics.get('user_tables_stats', []))

    pg_stat_activity_figs = build_pg_stat_activity_table(metrics['pg_stat_activity'])

    # Create a list of Graph components for each table chunk
    pg_stat_activity_rows = [dcc.Graph(figure=fig) for fig in pg_stat_activity_figs]

    return (historical_data, baseline_data, active_connections_fig, transactions_fig, blocks_fig,
            tuples_fig, cd_fig, temp_usage_fig, commits_over_time_fig, commits_per_second_fig,
            user_table_stats_fig, pg_stat_activity_rows)

if __name__ == '__main__':
    app.run_server(debug=True)
