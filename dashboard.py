import threading
import argparse

import dash

from dashboard.app import app
from dash import html, dcc

from dashboard.layouts.mongo_layout import mongodb_layout
from dashboard.layouts.postgres_layout import postgres_layout
from dashboard.collector.metrics_collector import MetricsCollector

import logging
from dashboard.logger.logging_config import logger

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Run the dashboard in verbose or silent mode.")
parser.add_argument("--mode", choices=["verbose", "silent"], default="silent",
                    help="Run the dashboard in 'verbose' or 'silent' mode.")
args = parser.parse_args()

# Adjust logging based on mode
if args.mode == "silent":
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            logger.removeHandler(handler)
            break

logger.info(f"Running in {args.mode} mode.")

app.layout = html.Div([
    dcc.Tabs(id="tabs", value="tab-postgres", children=[
        dcc.Tab(label="PostgreSQL", value="tab-postgres"),
        dcc.Tab(label="MongoDB", value="tab-mongodb")
    ]),
    html.Div(id="postgres-container", children=postgres_layout, style={'display': 'block'}),
    html.Div(id="mongodb-container", children=mongodb_layout, style={'display': 'none'})
])


@app.callback(
    dash.Output("postgres-container", "style"),
    dash.Output("mongodb-container", "style"),
    [dash.Input("tabs", "value")]
)
def show_hide_tab(value):
    logger.debug(f"Tab selected: {value}")
    if value == "tab-postgres":
        return {'display': 'block'}, {'display': 'none'}
    else:
        return {'display': 'none'}, {'display': 'block'}


from dashboard.callbacks.postgres_callbacks import *
from dashboard.callbacks.mongo_callbacks import *

def start_metrics_collector():
    """Function to start the metrics collector in a separate thread."""
    logger.info("Starting metrics collector.")
    collector = MetricsCollector()
    collector.start()


if __name__ == "__main__":
    # Start the metrics collector in a separate daemon thread
    collector_thread = threading.Thread(target=start_metrics_collector, daemon=True)
    collector_thread.start()

    # Run the Dash server
    logger.info("Starting the Dash server.")
    app.run_server(debug=True)
