from dash import html, dcc

# For the initial layout, we can create placeholders.
# The callbacks will update these figures later.
postgres_layout = html.Div([
    html.H1("PostgreSQL Metrics Dashboard", style={'textAlign': 'center', 'marginBottom': '30px'}),

    # Hidden stores to keep track of historical data and baseline data
    dcc.Store(id='historical-data', storage_type='memory', data=[]),
    dcc.Store(id='baseline-data', storage_type='memory', data=None),

    # Row 1: Active Connections, Transactions, Blocks, Tuples
    html.Div([
        html.Div([dcc.Graph(id='pg-active-connections')], style={'width': '24%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='pg-transactions')],
                 style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'}),
        html.Div([dcc.Graph(id='pg-blocks')], style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'}),
        html.Div([dcc.Graph(id='pg-tuples')], style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 2: Conflicts & Deadlocks, Temporary Usage
    html.Div([
        html.Div([dcc.Graph(id='pg-conflicts-deadlocks')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='pg-temp-usage')], style={'width': '49%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 3: Time-series charts
    html.Div([
        html.Div([dcc.Graph(id='pg-commits-over-time')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='pg-commits-per-second')],
                 style={'width': '49%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 4: User Table Stats
    html.Div([
        dcc.Graph(id='pg-user-tables-stats')
    ], style={'width': '100%', 'display': 'inline-block', 'marginBottom': '40px'}),

    # Row 5: Active Connections and Queries (pg_stat_activity table)
    html.Div(id='pg-stat-activity-rows', style={'marginBottom': '40px'}),

    # Row 5: Active Connections and Queries (pg_stat_activity table)
    html.Div(id='pg-stat-activity-rows', style={'marginBottom': '40px'}),

    # Row 6: Cache Hit Ratio Visualizations
    # In the Dash layout, these IDs will be updated by the callback
    html.Div([
        html.Div([dcc.Graph(id='pg-cache-hit-ratio-gauge')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='pg-cache-hit-ratio-line')],
                 style={'width': '49%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 7: Checkpoints and Buffer Writes
    html.Div([
        html.Div([dcc.Graph(id='pg-checkpoints-over-time')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='pg-buffer-writes-stacked')],
                 style={'width': '49%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 8: Index Usage Visualizations
    html.Div([
        html.Div([dcc.Graph(id='pg-index-usage-bar')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='pg-index-usage-combined')],
                 style={'width': '49%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Interval for updates (e.g. every 1 second)
    dcc.Interval(id='pg-interval', interval=1 * 1000, n_intervals=0)
])
