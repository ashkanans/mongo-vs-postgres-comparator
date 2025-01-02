import argparse
import traceback

from data.data_utils import read_movies_file
from db.simulator.mongodb_simulator import MongoSimulator
from db.simulator.postgresql_simulator import PostgresSimulator
from utils.config_loader import load_config
from utils.visualization import plot_results


def main():
    parser = argparse.ArgumentParser(description="Database Performance Comparison Tool")
    parser.add_argument(
        "actions",
        nargs="+",
        help="Actions to perform (e.g., setup, insertion, update, delete, visualize, bulk, many, one)",
    )
    parser.add_argument("--total_rows", type=int, default=100000, help="Total number of rows/documents to use")
    parser.add_argument("--bulk_size", type=int, default=1000, help="Bulk size for bulk operations")
    parser.add_argument("--persistent_connection", default=True, action="store_true", help="Use persistent connection")
    parser.add_argument("--concurrent", action="store_true", help="Run concurrent read/write operations test")
    parser.add_argument("--simulate_error", default=False, action="store_true",
                        help="Simulate an error in transaction to test rollback")
    parser.add_argument("--one", action="store_true", help="Update a single record")
    parser.add_argument("--many", action="store_true", help="Update multiple records")

    args = parser.parse_args()

    # Load configurations
    postgres_config = load_config('config/postgres_config.json')
    mongo_config = load_config('config/mongo_config.json')

    # Initialize simulators
    use_persistent_connection = args.persistent_connection
    print(f"Using persistent connection: {use_persistent_connection}")
    postgres_simulator = PostgresSimulator(postgres_config, use_persistent_connection, args.total_rows)
    mongo_simulator = MongoSimulator(mongo_config, use_persistent_connection, args.total_rows)

    # Perform setup if specified
    if "setup" in args.actions:
        print("Setting up databases...")
        postgres_simulator.setup()
        mongo_simulator.setup()
        print("Databases set up successfully.")

    # Read records
    file_path = "data/movies.txt"
    max_records = args.total_rows
    print(f"Using {max_records} records for the simulation")
    records = list(read_movies_file(file_path, max_records))

    try:
        if "insertion" in args.actions:
            if "one" in args.actions:
                print("Testing single insertion...")
                postgres_time, postgres_times = postgres_simulator.test_insertion(records)
                mongo_time, mongo_times = mongo_simulator.test_insertion(records)
                print(f"Insertion comparison: PostgreSQL: {postgres_time:.2f}s, MongoDB: {mongo_time:.2f}s.")
                if "visualize" in args.actions:
                    plot_results(postgres_time, postgres_times, mongo_time, mongo_times, operation_name="Insertion",
                                 use_persistent_connection=use_persistent_connection)

            if "bulk" in args.actions:
                bulk_size = args.bulk_size
                print(f"Testing bulk insertion with bulk size {bulk_size}...")
                postgres_time, postgres_times = postgres_simulator.test_insertion_many(records, bulk_size)
                mongo_time, mongo_times = mongo_simulator.test_insertion_many(records, bulk_size)
                print(f"Bulk insertion comparison: PostgreSQL: {postgres_time:.2f}s, MongoDB: {mongo_time:.2f}s.")
                if "visualize" in args.actions:
                    plot_results(postgres_time, postgres_times, mongo_time, mongo_times, operation_name="Insertion",
                                 bulk_size=bulk_size, use_persistent_connection=use_persistent_connection)

        if "update" in args.actions:
            if args.one:
                print("Testing single update...")
                # postgres_time, postgres_times = postgres_simulator.test_update_one()
                postgres_time, postgres_times = 0, 0
                mongo_time, mongo_times = mongo_simulator.test_update_one()
                print(f"Single update comparison:\n  PostgreSQL: {postgres_time:.2f}s\n  MongoDB: {mongo_time:.2f}s.")

                if "visualize" in args.actions:
                    plot_results(
                        postgres_time=postgres_time,
                        postgres_times=postgres_times,
                        mongo_time=mongo_time,
                        mongo_times=mongo_times,
                        operation_name="Update (One Record)",
                        use_persistent_connection=use_persistent_connection
                    )

            if args.many:
                bulk_size = args.bulk_size
                print(f"Testing bulk update with bulk size {bulk_size}...")
                postgres_time, postgres_times = postgres_simulator.test_update_many(bulk_size)
                mongo_time, mongo_times = mongo_simulator.test_update_many(bulk_size)
                print(f"Bulk update comparison:\n  PostgreSQL: {postgres_time:.2f}s\n  MongoDB: {mongo_time:.2f}s.")

                if "visualize" in args.actions:
                    plot_results(
                        postgres_time=postgres_time,
                        postgres_times=postgres_times,
                        mongo_time=mongo_time,
                        mongo_times=mongo_times,
                        operation_name="Update (Bulk)",
                        bulk_size=bulk_size,
                        use_persistent_connection=use_persistent_connection
                    )

        if "deletion" in args.actions:
            if args.one:
                print("Testing single delete...")
                postgres_time, postgres_times = postgres_simulator.test_delete_one()
                mongo_time, mongo_times = mongo_simulator.test_delete_one()
                print(f"Single delete comparison:\n  PostgreSQL: {postgres_time:.2f}s\n  MongoDB: {mongo_time:.2f}s.")

                if "visualize" in args.actions:
                    plot_results(
                        postgres_time=postgres_time,
                        postgres_times=postgres_times,
                        mongo_time=mongo_time,
                        mongo_times=mongo_times,
                        operation_name="Delete (One Record)",
                        use_persistent_connection=use_persistent_connection
                    )

            if args.many:
                bulk_size = args.bulk_size
                print(f"Testing bulk delete with bulk size {bulk_size}...")
                postgres_time, postgres_times = postgres_simulator.test_delete_many(bulk_size)
                mongo_time, mongo_times = mongo_simulator.test_delete_many(bulk_size)
                print(f"Bulk delete comparison:\n  PostgreSQL: {postgres_time:.2f}s\n  MongoDB: {mongo_time:.2f}s.")

                if "visualize" in args.actions:
                    plot_results(
                        postgres_time=postgres_time,
                        postgres_times=postgres_times,
                        mongo_time=mongo_time,
                        mongo_times=mongo_times,
                        operation_name="Delete (Bulk)",
                        bulk_size=bulk_size,
                        use_persistent_connection=use_persistent_connection
                    )

        if "concurrent" in args.actions:
            concurrency_level = 100
            num_operations = 1000000
            postgres_simulator.test_concurrent_operations(concurrency_level, num_operations)
            mongo_simulator.test_concurrent_operations(concurrency_level, num_operations)

        if "transaction" in args.actions:
            print("Testing transactional operations in MongoDB...")
            simulate_error = args.simulate_error
            postgres_time = postgres_simulator.test_transaction_operations(records, simulate_error)
            print(f"Transaction execution completed\n  PostgreSQL: {postgres_time:.2f}s\n")



    except Exception as e:
        print(f"An error occurred: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
