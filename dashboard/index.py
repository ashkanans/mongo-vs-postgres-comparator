import dash
from app import app
from dash import html, dcc

from layouts.mongo_layout import mongodb_layout
from layouts.postgres_layout import postgres_layout

# Create the overall layout with tabs
app.layout = html.Div([
    dcc.Tabs(id="tabs", value="tab-postgres", children=[
        dcc.Tab(label="PostgreSQL", value="tab-postgres"),
        dcc.Tab(label="MongoDB", value="tab-mongodb")
    ]),
    # Include both layouts at once
    html.Div(id="postgres-container", children=postgres_layout, style={'display': 'block'}),
    html.Div(id="mongodb-container", children=mongodb_layout, style={'display': 'none'})
])


# Callback to just show/hide the already-loaded layouts
@app.callback(
    dash.Output("postgres-container", "style"),
    dash.Output("mongodb-container", "style"),
    [dash.Input("tabs", "value")]
)
def show_hide_tab(value):
    if value == "tab-postgres":
        return {'display': 'block'}, {'display': 'none'}
    else:
        return {'display': 'none'}, {'display': 'block'}


from dashboard.callbacks.mongo_callbacks import *

if __name__ == "__main__":
    app.run_server(debug=True)
