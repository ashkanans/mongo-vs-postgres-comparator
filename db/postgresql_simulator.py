import time

from tqdm import tqdm

from db.postgres_handler import PostgresDBHandler


class PostgresSimulator:
    def __init__(self, config, use_persistent_connection=False):
        self.handler = PostgresDBHandler(config, use_persistent_connection)

    def setup(self):
        """Set up the database and table for testing."""
        print("Setting up PostgreSQL database and table...")
        self.handler.create_database()
        self.handler.create_reviews_table()
        print("PostgreSQL setup complete.")

    def test_insertion(self, records):
        """Test insertion of records into PostgreSQL with a progress bar."""
        print("Testing PostgreSQL insertion...")
        start_time = time.time()
        individual_times = []

        # Use tqdm to create a progress bar
        for record in tqdm(records, desc="Inserting Records", unit="record"):
            record_start = time.time()  # Record the start time for this insertion
            self.handler.insert_one(record)
            record_end = time.time()
            individual_times.append(record_end - record_start)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Inserted {len(records)} records into PostgreSQL in {total_time:.2f} seconds.")
        return total_time, individual_times

    def test_query_performance(self, query):
        """Test query performance in PostgreSQL."""
        print("Testing PostgreSQL query performance...")
        conn = self.handler._connect()
        cursor = conn.cursor()
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Query completed in {total_time:.2f} seconds, returned {len(results)} rows.")
        cursor.close()
        conn.close()
        return total_time, results

    def test_index_performance(self, column):
        """Test performance with and without index."""
        print(f"Testing PostgreSQL index performance on column '{column}'...")
        # Without Index
        print("Testing without index...")
        query = f"SELECT * FROM reviews WHERE {column} = 'some_value';"
        no_index_time, _ = self.test_query_performance(query)

        # With Index
        print("Creating index and testing with index...")
        self.handler.create_single_column_index('reviews', column)
        index_time, _ = self.test_query_performance(query)

        print(f"Performance comparison: Without Index: {no_index_time:.2f}s, With Index: {index_time:.2f}s.")
        return no_index_time, index_time

    def test_insertion_many(self, records, bulk_size=-1):
        """Test bulk insertion of records into PostgreSQL."""
        print("Testing PostgreSQL bulk insertion...")
        start_time = time.time()
        individual_times = []
        total_records = len(records)

        # Determine bulk size
        if bulk_size == -1:
            # Insert all records as one bulk
            bulk_size = total_records
            print("Inserting all records in a single bulk.")

        # Split the data into chunks based on the bulk size
        for i in tqdm(range(0, total_records, bulk_size), desc="Inserting Bulk Records", unit="bulk"):
            bulk_start = time.time()
            bulk = records[i:i + bulk_size]
            self.handler.insert_many(bulk)
            bulk_end = time.time()
            individual_times.append(bulk_end - bulk_start)

        end_time = time.time()
        total_time = end_time - start_time
        print(
            f"Inserted {total_records} records into PostgreSQL in {total_time:.2f} seconds using bulk size {bulk_size}.")
        return total_time, individual_times

    def ensure_empty(self, table_name="reviews"):
        """Ensure that the PostgreSQL table is empty."""
        if not self.handler.is_empty(table_name):
            print(f"PostgreSQL table '{table_name}' is not empty. Dropping and recreating...")
            self.handler.create_reviews_table()
            print(f"PostgreSQL table '{table_name}' has been recreated.")
