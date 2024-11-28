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

        # Use tqdm to create a progress bar
        for record in tqdm(records, desc="Inserting Records", unit="record"):
            self.handler.insert_one(record)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Inserted {len(records)} records into PostgreSQL in {total_time:.2f} seconds.")
        return total_time

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
