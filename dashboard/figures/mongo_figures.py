import plotly.graph_objs as go
from dash import dcc


class MongoFigures:

    @staticmethod
    def active_connections_gauge(connections_data):
        """
        Creates a gauge figure for active connections.
        Expected connections_data format:
        {
            'current': int,
            'available': int,
            'totalCreated': int
        }
        """
        current = connections_data.get('current', 0)
        available = connections_data.get('available', 0)

        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=current,
            gauge={'axis': {'range': [0, current + available if available > 0 else current + 10]}},
            title={'text': "Active Connections"}
        ))
        return fig

    @staticmethod
    def opcounters_graph(relative_opcounters):
        """
        Creates a bar chart of relative operation counters.
        Expected relative_opcounters format:
        {
            'insert': int,
            'query': int,
            'update': int,
            'delete': int,
            'getmore': int,
            'command': int
        }
        """
        operations = list(relative_opcounters.keys())
        values = list(relative_opcounters.values())

        fig = go.Figure([go.Bar(x=operations, y=values)])
        fig.update_layout(
            title="Operation Counters (Relative to Baseline)",
            xaxis_title="Operation Type",
            yaxis_title="Count"
        )
        return fig

    @staticmethod
    def network_usage_graph(network_data):
        """
        Creates a small bar or line chart for network usage.
        Expected network_data format:
        {
            'bytesIn': int,
            'bytesOut': int,
            'numRequests': int
        }
        """
        metrics = ['bytesIn', 'bytesOut', 'numRequests']
        values = [network_data.get(m, 0) for m in metrics]

        fig = go.Figure([go.Bar(x=metrics, y=values)])
        fig.update_layout(
            title="Network Usage",
            xaxis_title="Metric",
            yaxis_title="Value"
        )
        return fig

    @staticmethod
    def memory_usage_graph(mem_data):
        """
        Creates a bar chart for memory usage.
        Expected mem_data format (depends on MongoDB version):
        {
            'resident': int,
            'virtual': int,
            'mapped': int,
            'mappedWithJournal': int
        }
        Note: Some fields may be missing depending on MongoDB version.
        """
        metrics = [k for k in mem_data.keys()]
        values = [mem_data.get(m, 0) for m in metrics]

        fig = go.Figure([go.Bar(x=metrics, y=values)])
        fig.update_layout(
            title="Memory Usage",
            xaxis_title="Memory Metric",
            yaxis_title="Value (MB or count)"
        )
        return fig

    @staticmethod
    def db_size_graph(db_stats):
        """
        Creates a bar chart for database size metrics.
        Expected db_stats format:
        {
            'dataSize': float,
            'storageSize': float,
            'indexSize': float,
            ...
        }
        """
        metrics = ['dataSize', 'storageSize', 'indexSize']
        values = [db_stats.get(m, 0) for m in metrics]

        fig = go.Figure([go.Bar(x=metrics, y=values)])
        fig.update_layout(
            title=f"Database Size: {db_stats.get('db', '')}",
            xaxis_title="Metric",
            yaxis_title="Bytes"
        )
        return fig

    @staticmethod
    def ops_over_time_line_chart(historical_data, metric, title):
        """
        Plots a line chart of a given metric over time from historical data.
        Assumes historical_data is a list of metric snapshots over time.
        For example, plotting sum of all opcounters over time.

        metric could be 'opcounters' and we'll sum them or pick a specific key.
        """
        if not historical_data:
            return go.Figure()

        times = [d['timestamp'] for d in historical_data]
        # Let's assume we sum insert+query+update+delete+getmore+command for total ops
        values = []
        for d in historical_data:
            ops = d.get('server_status', {}).get('opcounters', {})
            total_ops = sum(ops.get(k, 0) for k in ops.keys())
            values.append(total_ops)

        fig = go.Figure([go.Scatter(x=times, y=values, mode='lines+markers')])
        fig.update_layout(
            title=title,
            xaxis_title="Time",
            yaxis_title="Operation Count"
        )
        return fig

    @staticmethod
    def current_operations_table(current_operations):
        """
        Displays current operations as a table.
        current_operations might be a dict returned from currentOp command.
        For example:
        {
            "inprog": [
                {"opid": 123, "ns": "db.collection", "query": {...}, "secs_running": 10, ...},
                ...
            ]
        }
        """
        inprog = current_operations.get('inprog', [])
        if not inprog:
            return [dcc.Markdown("No current operations")]

        # Extract some columns from each operation for display
        columns = ["opid", "ns", "secs_running", "op"]
        table_data = {col: [] for col in columns}

        for op in inprog:
            for col in columns:
                table_data[col].append(op.get(col, 'N/A'))

        fig = go.Figure(data=[go.Table(
            header=dict(values=columns, fill_color='lightgray', align='left'),
            cells=dict(values=[table_data[col] for col in columns], align='left')
        )])

        # Wrap figure in a Div or return directly as Graph:
        return [dcc.Graph(figure=fig)]

    @staticmethod
    def top_metrics_chart(top_metrics):
        """
        Creates a bar chart showing top metrics (per-collection operation time).
        top_metrics format (from 'top' command):
        {
            "totals": {
                "db.collection": {
                    "total": {"time": ...},
                    "readLock": {"count": ..., "time": ...},
                    "writeLock": {"count": ..., "time": ...}
                },
                ...
            }
        }
        We'll plot total_time for each collection.
        """
        totals = top_metrics.get('totals', {})
        collections = []
        total_times = []

        for coll, stats in totals.items():
            total_time = stats.get('total', {}).get('time', 0)
            if '.' in coll:  # likely format 'db.collection'
                collections.append(coll)
                total_times.append(total_time)

        fig = go.Figure([go.Bar(x=collections, y=total_times)])
        fig.update_layout(
            title="Top Metrics (Total Operation Time per Collection)",
            xaxis_title="Collection",
            yaxis_title="Total Time (microseconds)"
        )
        return fig

    @staticmethod
    def cache_hit_ratio_over_time(historical_data):
        """
        Plots the WiredTiger cache hit ratio over time if available.
        Looks at server_status['wiredTiger']['cache']['cache_hit_percent'] (if it exists)
        """
        if not historical_data:
            return go.Figure()

        times = []
        ratios = []
        for d in historical_data:
            cache = d.get('server_status', {}).get('wiredTiger', {}).get('cache', {})
            # Depending on version, might be 'cache_hit_percent' or 'hit ratio'
            ratio = cache.get('cache_hit_percent', None)
            if ratio is not None:
                times.append(d['timestamp'])
                ratios.append(ratio)

        if not times or not ratios:
            # If no data available, return empty fig
            return go.Figure()

        fig = go.Figure([go.Scatter(x=times, y=ratios, mode='lines+markers')])
        fig.update_layout(
            title="WiredTiger Cache Hit Ratio Over Time",
            xaxis_title="Time",
            yaxis_title="Cache Hit Ratio (%)"
        )
        return fig

    @staticmethod
    def cumulative_and_rate_graph(historical_data, operation):
        """
        Creates a combined chart showing both cumulative and rate data for a specific operation.
        """
        # Extract timestamps and operation counters
        timestamps = [entry['timestamp'] for entry in historical_data]
        opcounters_list = [entry.get('server_status', {}).get('opcounters', {}) for entry in historical_data]

        # Compute cumulative data
        cumulative_data = [opc.get(operation, 0) for opc in opcounters_list]

        # Compute rate data (operations per second)
        rate_data = []
        for i in range(1, len(cumulative_data)):
            time_diff = timestamps[i] - timestamps[i - 1]
            rate = (cumulative_data[i] - cumulative_data[i - 1]) / time_diff if time_diff > 0 else 0
            rate_data.append(rate)

        # Remove the first timestamp for the rate data
        rate_timestamps = timestamps[1:]

        # Create the figure
        fig = go.Figure()

        # Add cumulative line trace
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=cumulative_data,
            mode='lines+markers',
            name=f'{operation.capitalize()} - Cumulative',
            yaxis='y1'
        ))

        # Add rate line trace
        fig.add_trace(go.Scatter(
            x=rate_timestamps,
            y=rate_data,
            mode='lines+markers',
            name=f'{operation.capitalize()} - Rate',
            yaxis='y2'
        ))

        # Update layout to have dual y-axes
        fig.update_layout(
            title=f"{operation.capitalize()} Operations (Cumulative and Rate)",
            xaxis_title="Time",
            yaxis=dict(title="Cumulative Count", side='left'),
            yaxis2=dict(title="Rate (Ops/sec)", overlaying='y', side='right'),
            xaxis=dict(tickformat='%Y-%m-%d %H:%M:%S')
        )

        return fig
