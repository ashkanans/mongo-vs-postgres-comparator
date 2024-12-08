import pandas as pd
import plotly.express as px
from dash import html, dcc

# Example: Dummy data for MongoDB metrics
df = pd.DataFrame({
    "metric": ["Ops/s", "Connections", "Memory Usage"],
    "value": [300, 45, 1024]
})

fig = px.bar(df, x="metric", y="value", title="MongoDB Metrics Overview")

mongodb_layout = html.Div([
    html.H2("MongoDB Dashboard"),
    dcc.Graph(figure=fig)
])
