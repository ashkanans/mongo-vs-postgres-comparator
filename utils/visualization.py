from matplotlib import pyplot as plt


def plot_operation_comparison(times, number_of_records, operation_name, use_persistent_connection):
    """
    Plot a bar chart comparing operation times (e.g., insertion, update, deletion) for different databases.

    :param times: Dictionary containing operation times for each database (e.g., {'PostgreSQL': 10, 'MongoDB': 8}).
    :param number_of_records: Number of records processed during the operation.
    :param operation_name: Name of the operation being compared (e.g., "Insertion", "Update", "Deletion").
    :param use_persistent_connection: Whether a persistent connection was used (boolean).
    """
    dbs = list(times.keys())
    durations = list(times.values())
    colors = ['blue', 'green', 'orange', 'red']  # Define colors for different databases (expandable)

    plt.figure(figsize=(8, 6))
    bars = plt.bar(dbs, durations, color=colors[:len(dbs)])  # Use as many colors as there are databases

    # Add legend
    plt.legend(bars, dbs, title="Databases")

    # Label axes and add title
    plt.xlabel("Database")
    plt.ylabel(f"{operation_name} Time (seconds) for {number_of_records} records")
    plt.title(f"Database {operation_name} Time Comparison (Persistent connection: {use_persistent_connection})")

    # Annotate bars with durations
    for bar, duration in zip(bars, durations):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                 f'{duration:.2f}', ha='center', va='bottom')

    # Display the plot
    plt.show(block=True)


def plot_operation_times(record_times, db_name, operation_name, use_persistent_connection):
    """
    Plot the time taken for each database operation.

    :param record_times: List of times for each record operation (insertion, update, deletion, etc.).
    :param db_name: Name of the database (e.g., "PostgreSQL", "MongoDB").
    :param operation_name: Name of the operation (e.g., "Insertion", "Update", "Deletion").
    :param use_persistent_connection: Whether a persistent connection was used (boolean).
    """
    plt.figure(figsize=(10, 6))
    plt.plot(record_times, label=f"{db_name} ({operation_name})", marker='o')
    plt.xlabel("Record Number")
    plt.ylabel(f"{operation_name} Time (seconds)")
    plt.title(
        f"Individual Record {operation_name} Times for {db_name} \n(Persistent connection: {use_persistent_connection})")
    plt.legend()
    plt.grid()
    plt.show(block=True)


def plot_results(postgres_time, postgres_times, mongo_time, mongo_times, operation_name, bulk_size=None,
                 use_persistent_connection=False):
    """
    Plot the results for operation performance comparison.

    :param postgres_time: Total time for the operation in PostgreSQL.
    :param postgres_times: List of times for individual operations in PostgreSQL.
    :param mongo_time: Total time for the operation in MongoDB.
    :param mongo_times: List of times for individual operations in MongoDB.
    :param operation_name: Name of the operation (e.g., "Insertion", "Update", "Deletion").
    :param bulk_size: Bulk size for bulk operations (optional).
    :param use_persistent_connection: Whether a persistent connection was used (boolean).
    """
    # Plot total operation comparison
    times = {"PostgreSQL": postgres_time, "MongoDB": mongo_time}
    number_of_records = len(postgres_times) if postgres_times else 0
    operation_display_name = f"{operation_name} (Bulk Size: {bulk_size})" if bulk_size else operation_name

    print(f"Plotting total time comparison for {operation_display_name}...")
    plot_operation_comparison(
        times=times,
        number_of_records=number_of_records,
        operation_name=operation_display_name,
        use_persistent_connection=use_persistent_connection
    )

    # Plot individual operation times
    print(f"Plotting individual operation times for PostgreSQL and MongoDB ({operation_display_name})...")
    plot_operation_times(
        record_times=postgres_times,
        db_name="PostgreSQL",
        operation_name=operation_display_name,
        use_persistent_connection=use_persistent_connection
    )
    plot_operation_times(
        record_times=mongo_times,
        db_name="MongoDB",
        operation_name=operation_display_name,
        use_persistent_connection=use_persistent_connection
    )
