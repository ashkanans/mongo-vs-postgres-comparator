import dash
from app import app
from dash import html, dcc

from layouts.mongodb_layout import mongodb_layout
from layouts.postgres_layout import postgres_layout

# Create the overall layout with tabs
app.layout = html.Div([
    dcc.Tabs(id="tabs", value="tab-postgres", children=[
        dcc.Tab(label="PostgreSQL", value="tab-postgres"),
        dcc.Tab(label="MongoDB", value="tab-mongodb")
    ]),
    html.Div(id="tab-content")
])


# Callback to switch tabs
@app.callback(
    dash.Output("tab-content", "children"),
    [dash.Input("tabs", "value")]
)
def render_content(tab):
    if tab == "tab-postgres":
        return postgres_layout  # Return the layout object, not the module
    elif tab == "tab-mongodb":
        return mongodb_layout  # Return the layout object, not the module
    return html.Div("No tab selected")


from dashboard.callbacks.mongodb_callbacks import *


if __name__ == "__main__":
    app.run_server(debug=True)
