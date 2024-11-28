from matplotlib import pyplot as plt


def plot_insertion_times(times, number_of_records, use_persistent_connection):
    """
    Plot a bar chart comparing insertion times for PostgreSQL and MongoDB with legend and different bar colors.

    :param times: Dictionary containing insertion times for each database.
    """
    dbs = list(times.keys())
    durations = list(times.values())
    colors = ['blue', 'green']  # Define different colors for each database

    plt.figure(figsize=(8, 6))
    bars = plt.bar(dbs, durations, color=colors)

    # Add legend
    plt.legend(bars, dbs, title="Databases")

    # Label axes and add title
    plt.xlabel("Database")
    plt.ylabel(f"Insertion Time (seconds) for {number_of_records} records in bulks")
    plt.title(f"Database Bulk Insertion Time Comparison (Persistent connection: {use_persistent_connection})")

    # Display the plot
    plt.show(block=True)


def plot_record_insertion_times(record_times, db_name, use_persistent_connection):
    """
    Plot the time taken for each record insertion.

    :param record_times: List of times for each record insertion.
    :param db_name: Name of the database (e.g., "PostgreSQL" or "MongoDB").
    """
    plt.figure(figsize=(10, 6))
    plt.plot(record_times, label=db_name, marker='o')
    plt.xlabel("Record Number")
    plt.ylabel("Insertion Time (seconds)")
    plt.title(f"Individual Record Insertion Times for {db_name} (Persistent connection: {use_persistent_connection})")
    plt.legend()
    plt.grid()
    plt.show(block=True)
