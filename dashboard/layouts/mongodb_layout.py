from dash import html, dcc

mongodb_layout = html.Div([
    html.H1("MongoDB Metrics Dashboard", style={'textAlign': 'center', 'marginBottom': '30px'}),

    # Hidden stores to keep track of historical data and baseline data
    dcc.Store(id='historical-mongo-data', storage_type='memory', data=[]),
    dcc.Store(id='baseline-mongo-data', storage_type='memory', data=None),

    # Row 1: Active Connections, Opcounters, Network Usage, Memory Usage
    html.Div([
        html.Div([dcc.Graph(id='mongo-active-connections')], style={'width': '24%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='mongo-opcounters')],
                 style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'}),
        html.Div([dcc.Graph(id='mongo-network-usage')],
                 style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'}),
        html.Div([dcc.Graph(id='mongo-memory-usage')],
                 style={'width': '24%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 2: Database Size and Operations Over Time
    html.Div([
        html.Div([dcc.Graph(id='mongo-db-size')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='mongo-ops-over-time')],
                 style={'width': '49%', 'display': 'inline-block', 'marginLeft': '1%'})
    ], style={'marginBottom': '40px'}),

    # Row 3: Current Operations Table
    html.Div([
        html.Div(id='mongo-current-operations', style={'width': '100%', 'marginBottom': '40px'})
    ]),

    # Row 4: Top Metrics
    html.Div([
        html.Div([dcc.Graph(id='mongo-top-metrics')], style={'width': '100%', 'display': 'inline-block'})
    ], style={'marginBottom': '40px'}),

    # Row 5: Cache Hit Ratio Over Time
    html.Div([
        html.Div([dcc.Graph(id='mongo-cache-over-time')], style={'width': '100%', 'display': 'inline-block'})
    ], style={'marginBottom': '40px'}),

    # Interval for updates (e.g., every 1 second)
    dcc.Interval(id='mongo-interval', interval=1 * 1000, n_intervals=0)
])
