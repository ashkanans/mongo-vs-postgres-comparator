from dash import Dash

# Initialize the Dash app
app = Dash(__name__, suppress_callback_exceptions=True, title="Metrics Dashboard")
server = app.server
