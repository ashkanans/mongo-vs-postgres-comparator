import time

from data.data_utils import read_movies_file
from db.mongodb_handler import test_mongo_connection
from db.postgres_handler import test_postgres_connection


def test_configurations(postgres_config, mongo_config):
    """Test database configurations for PostgreSQL and MongoDB."""
    # Test PostgreSQL connection
    print("Testing PostgreSQL configuration...")
    if test_postgres_connection(postgres_config):
        print("PostgreSQL configuration is correct.")
    else:
        print("Failed to connect to PostgreSQL. Check your configuration.")

    # Test MongoDB connection
    print("Testing MongoDB configuration...")
    if test_mongo_connection(mongo_config):
        print("MongoDB configuration is correct.")
    else:
        print("Failed to connect to MongoDB. Check your configuration.")


def measure_insertion_time(db_name, insert_function, config, file_path, max_records):
    """
    Measure the time taken to insert a specified number of records into a database.

    :param db_name: Name of the database (e.g., "PostgreSQL" or "MongoDB").
    :param insert_function: The function used to insert records into the database.
    :param config: Configuration dictionary for the database.
    :param file_path: Path to the data file.
    :param max_records: Maximum number of records to insert.
    :return: Tuple of total insertion time and list of times for each record.
    """
    print(f"Starting insertion into {db_name}...")
    record_times = []
    record_count = 0
    start_time = time.time()

    for record in read_movies_file(file_path):
        record_start_time = time.time()
        insert_function(config, record)
        record_end_time = time.time()

        # Record the time for this insertion
        record_times.append(record_end_time - record_start_time)
        record_count += 1
        if record_count >= max_records:
            break

    end_time = time.time()
    total_time = end_time - start_time
    print(f"{db_name} insertion completed. Total records: {record_count}. Time taken: {total_time:.2f} seconds.")
    return total_time, record_times
