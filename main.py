from data.data_utils import read_movies_file
from db.mongodb_simulator import MongoSimulator
from db.postgresql_simulator import PostgresSimulator
from utils.config_loader import load_config
from utils.visualization import plot_insertion_times, plot_record_insertion_times


def main():
    # Load configurations
    postgres_config = load_config('config/postgres_config.json')
    mongo_config = load_config('config/mongo_config.json')

    # Initialize simulators
    use_persistent_connection = True
    print(f"Testing with presistent connection: {use_persistent_connection}")
    postgres_simulator = PostgresSimulator(postgres_config, use_persistent_connection)
    mongo_simulator = MongoSimulator(mongo_config, use_persistent_connection)

    # Set up databases
    print("Setting up databases...")
    postgres_simulator.setup()
    mongo_simulator.setup()
    print("Databases set up successfully.")

    # Read records
    file_path = "data/movies.txt"
    max_records = 100000
    print(f"{max_records} records are used for the simulation")
    records = list(read_movies_file(file_path, max_records))

    # # Test insertion
    # print("Testing insertion performance...")
    # postgres_time, postgres_times = postgres_simulator.test_insertion(records)
    # mongo_time, mongo_times = mongo_simulator.test_insertion(records)
    #
    # # Compare insertion times
    # print(f"Insertion comparison: PostgreSQL: {postgres_time:.2f}s, MongoDB: {mongo_time:.2f}s.")
    # plot_insertion_times({"MongoDB": mongo_time, "PostgreSQL": postgres_time}, max_records, use_persistent_connection)
    # plot_record_insertion_times(mongo_times, "MongoDB", use_persistent_connection)
    # plot_record_insertion_times(postgres_times, "PostgreSQL", use_persistent_connection)

    # Test insertion many
    bulks = 1000
    print("Testing insertion (bulk) performance...")
    print(f"{bulks} number of bulks")
    postgres_time, postgres_times = postgres_simulator.test_insertion_many(records, bulks)
    mongo_time, mongo_times = mongo_simulator.test_insertion_many(records, bulks)

    # Compare insertion times
    print(f"Insertion (bulk) comparison: PostgreSQL: {postgres_time:.2f}s, MongoDB: {mongo_time:.2f}s.")
    plot_insertion_times({"MongoDB": mongo_time, "PostgreSQL": postgres_time}, max_records, use_persistent_connection)
    plot_record_insertion_times(mongo_times, "MongoDB", use_persistent_connection)
    plot_record_insertion_times(postgres_times, "PostgreSQL", use_persistent_connection)

    # # Test query performance
    # query = "SELECT * FROM reviews WHERE product_id = 'some_value';"
    # postgres_query_time, _ = postgres_simulator.test_query_performance(query)
    # mongo_query_time, _ = mongo_simulator.test_query_performance({"product_id": "some_value"})
    #
    # print(f"Query comparison: PostgreSQL: {postgres_query_time:.2f}s, MongoDB: {mongo_query_time:.2f}s.")
    #
    # # Test index performance
    # postgres_index_times = postgres_simulator.test_index_performance('product_id')
    # mongo_index_times = mongo_simulator.test_index_performance('product_id')
    #
    # print("Performance with and without index:")
    # print(f"PostgreSQL: Without Index: {postgres_index_times[0]:.2f}s, With Index: {postgres_index_times[1]:.2f}s.")
    # print(f"MongoDB: Without Index: {mongo_index_times[0]:.2f}s, With Index: {mongo_index_times[1]:.2f}s.")


if __name__ == "__main__":
    main()
