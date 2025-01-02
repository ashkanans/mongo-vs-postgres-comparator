import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from db.handler.postgres_handler import PostgresDBHandler
from utils.db_utils import normalize_record


class PostgresSimulator:
    def __init__(self, config, use_persistent_connection=False, total_records=None):
        self.total_records = total_records
        self.handler = PostgresDBHandler(config)
        self.modified = 0
        self.inserted = 0
        self.deleted = 0

    def setup(self):
        """Set up the database and table for testing."""
        print("Setting up PostgreSQL database and table...")
        self.handler.create_database()
        self.handler.create_reviews_table()
        print("PostgreSQL setup complete.")

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

    # Insertion methods
    def test_insertion(self, records):
        self.validate_before_executing("insertion")
        print("Testing PostgreSQL insertion...")
        start_time = time.time()
        individual_times = []

        for record in tqdm(records, desc="Inserting Records", unit="record"):
            normalized_record = normalize_record(record)
            record_start = time.time()
            self.handler.insert_one(normalized_record)
            self.inserted += 1
            record_end = time.time()
            individual_times.append(record_end - record_start)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Inserted {len(records)} records into PostgreSQL in {total_time:.2f} seconds.")
        return total_time, individual_times

    def test_insertion_many(self, records, bulk_size=-1):
        self.validate_before_executing("insertion")
        print("Testing PostgreSQL bulk insertion...")
        start_time = time.time()
        individual_times = []

        formatted_records = [normalize_record(record) for record in records]
        total_records = len(formatted_records)

        if bulk_size == -1:
            bulk_size = total_records
            print("Inserting all records in a single bulk.")

        for i in tqdm(range(0, total_records, bulk_size), desc="Inserting Bulk Records", unit="bulk"):
            bulk_start = time.time()
            bulk = formatted_records[i:i + bulk_size]
            self.inserted += self.handler.insert_many(bulk)
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

    # Update methods
    def test_update_one(self):
        self.validate_before_executing("update")
        print("Testing PostgreSQL update one...")
        postgres_times = []

        try:
            ids = self.handler.get_all_review_ids()
            print(f"Retrieved {len(ids)} IDs from the `reviews` table.")

            start_time = time.time()
            for review_id in tqdm(ids, desc="Updating Records", unit="record"):
                update_query = f"UPDATE reviews SET score = score + 0.123 WHERE id = {review_id}"
                op_start = time.time()
                self.handler.update_one(update_query)
                op_end = time.time()
                postgres_times.append(op_end - op_start)

            end_time = time.time()
            postgres_time = end_time - start_time

            print(f"Update one operation completed in {postgres_time:.2f} seconds.")
            return postgres_time, postgres_times

        except Exception as e:
            print(f"Error during the update process: {e}")
            return None, []

    def test_update_many(self, bulk_size=-1):
        self.validate_before_executing("update")
        print("Testing PostgreSQL update many with bulk size...")
        postgres_times = []

        try:
            ids = self.handler.get_all_review_ids()
            total_ids = len(ids)
            print(f"Retrieved {total_ids} IDs from the `reviews` table.")

            if bulk_size == -1:
                bulk_size = total_ids
                print("Executing all updates in a single bulk.")

            start_time = time.time()
            for i in tqdm(range(0, total_ids, bulk_size), desc="Updating Many Records", unit="bulk"):
                bulk_ids = ids[i:i + bulk_size]
                bulk_queries = [{"filter_query": (review_id,)} for review_id in bulk_ids]
                bulk_start = time.time()
                self.modified += self.handler.update_many_bulk(bulk_queries)
                bulk_end = time.time()
                postgres_times.append(bulk_end - bulk_start)

            end_time = time.time()
            postgres_time = end_time - start_time

            print(f"Update many operation completed in {postgres_time:.2f} seconds with bulk size {bulk_size}.")
            return postgres_time, postgres_times

        except Exception as e:
            print(f"Error during the bulk update process: {e}")
            return None, []

    def test_delete_one(self):
        self.validate_before_executing("delete")
        """Test deleting a single record in PostgreSQL based on IDs retrieved from the table."""
        print("Testing PostgreSQL delete one...")
        postgres_times = []

        try:
            # Retrieve all IDs from the `reviews` table
            delete_ids = self.handler.get_all_review_ids()
            total_ids = len(delete_ids)
            print(f"Retrieved {total_ids} IDs for deletion.")

            start_time = time.time()

            for record_id in tqdm(delete_ids, desc="Deleting One Record", unit="query"):
                op_start = time.time()
                self.handler.delete_one(record_id)
                op_end = time.time()

                postgres_times.append(op_end - op_start)

            end_time = time.time()
            postgres_time = end_time - start_time

            print(f"Delete one operation completed in {postgres_time:.2f} seconds.")
            return postgres_time, postgres_times

        except Exception as e:
            print(f"Error during the delete one process: {e}")
            return None, []

    def test_delete_many(self, bulk_size=-1):
        self.validate_before_executing("delete")
        """Test deleting multiple records in PostgreSQL with bulk execution."""
        print("Testing PostgreSQL delete many with bulk size...")
        postgres_times = []

        try:
            # Retrieve all IDs from the `reviews` table
            delete_ids = self.handler.get_all_review_ids()
            total_ids = len(delete_ids)
            print(f"Retrieved {total_ids} IDs for deletion.")

            if bulk_size == -1:
                bulk_size = total_ids  # Execute all deletes in a single bulk
                print("Executing all delete queries in a single bulk.")

            start_time = time.time()

            for i in tqdm(range(0, total_ids, bulk_size), desc="Deleting Many Records", unit="bulk"):
                bulk_ids = delete_ids[i:i + bulk_size]

                # Execute the bulk delete
                bulk_start = time.time()
                self.handler.delete_many_bulk(bulk_ids)
                bulk_end = time.time()

                postgres_times.append(bulk_end - bulk_start)

            end_time = time.time()
            postgres_time = end_time - start_time

            print(f"Delete many operation completed in {postgres_time:.2f} seconds with bulk size {bulk_size}.")
            return postgres_time, postgres_times

        except Exception as e:
            print(f"Error during the bulk delete process: {e}")
            return None, []

    def validate_before_executing(self, action):
        """
        Validates the state of the simulator before executing the specified action.

        :param action: The action to validate (e.g., "insertion", "update", "delete").
        :raises Exception: If the validation fails, an appropriate error message is raised.
        """
        print(f"Validating simulator state before executing '{action}'...")

        if action == "insertion":
            if self.modified != 0 or self.inserted != 0 or self.deleted != 0:
                raise Exception(
                    "Validation failed for 'insertion': Simulator state must have modified=0, inserted=0, and deleted=0."
                )

        elif action == "update":
            if self.modified != 0 or self.inserted != self.total_records or self.deleted != 0:
                raise Exception(
                    "Validation failed for 'update': Simulator state must have modified=0, "
                    f"inserted={self.total_records}, and deleted=0."
                )

        elif action == "delete":
            if self.inserted != self.total_records or self.deleted != 0:
                raise Exception(
                    "Validation failed for 'delete': Simulator state must have inserted={self.total_records} "
                    "and deleted=0."
                )

        else:
            raise Exception(f"Unknown action '{action}' specified for validation.")

        print(f"Validation passed for action '{action}'.")

    # Cocurrency test methods
    def read_one_by_id(self, record_id):
        conn = self.handler._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM reviews WHERE id = %s", (record_id,))
        cursor.fetchall()
        cursor.close()
        self.handler._close_connection(conn)

    def test_concurrent_operations(self, concurrency_level=10, num_operations=100):
        """
        Perform concurrent read, write, and update operations to test PostgreSQL under load.

        :param concurrency_level: Number of concurrent threads to use.
        :param num_operations: Total number of operations to perform.
        """
        print(
            f"Testing concurrent operations with {concurrency_level} threads and {num_operations} total operations...")

        # Retrieve all IDs for read/update operations
        ids = self.handler.get_all_review_ids()
        if not ids:
            print("No IDs found in the `reviews` table. Ensure data is inserted before running concurrency tests.")
            return

        # Define tasks: mix of reads, updates, and inserts
        def read_operation():
            chosen_id = random.choice(ids)
            self.read_one_by_id(chosen_id)

        def write_operation():
            record = {
                "product/productId": f"Product{random.randint(1, 1000)}",
                "review/userId": f"User{random.randint(1, 1000)}",
                "review/profileName": f"User{random.randint(1, 1000)}",
                "review/helpfulness": "0/0",
                "review/score": random.uniform(1, 5),
                "review/time": int(time.time()),
                "review/summary": "Sample Summary",
                "review/text": "Sample Review Text"
            }
            self.handler.insert_one(record)

        def update_operation():
            random_id = random.choice(ids)
            update_query = f"UPDATE reviews SET score = score + 0.123 WHERE id = {random_id}"
            self.handler.update_one(update_query)

        # Create a mix of read (60%), write (20%), and update (20%) tasks
        tasks = []
        for _ in range(num_operations):
            rand = random.random()
            if rand < 0.6:
                tasks.append(read_operation)  # 60% reads
            elif rand < 0.8:
                tasks.append(write_operation)  # 20% writes
            else:
                tasks.append(update_operation)  # 20% updates

        # Execute tasks concurrently with a progress bar
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=concurrency_level) as executor:
            futures = [executor.submit(task) for task in tasks]
            for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing Tasks", unit="task"):
                pass  # Each future is processed as it completes

        end_time = time.time()
        print(f"Concurrent operations completed in {end_time - start_time:.2f} seconds.")

    def test_transaction_operations(self, records, simulate_error=False):
        """
        Test transactional operations in PostgreSQL.

        :param records: The records to operate on.
        :param simulate_error: Whether to simulate an error to test rollback behavior.
        :return: Total execution time.
        """
        print("Testing PostgreSQL transactional operations...")
        conn = None
        start_time = time.time()

        try:
            # Step 1: Start a transaction
            conn = self.handler._get_connection()
            cursor = conn.cursor()
            conn.autocommit = False  # Disable autocommit for transactional behavior
            print("Transaction started...")

            # Step 2: Perform multiple insertions within the transaction with progress bar
            print("Inserting records within a transaction...")
            insert_query = """
            INSERT INTO reviews (
                product_id, user_id, profile_name, helpfulness, score, review_time, summary, review_text
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            for record in tqdm(records, desc="Inserting Records", unit="record"):
                cursor.execute(insert_query, (
                    record.get("product/productId"),
                    record.get("review/userId"),
                    record.get("review/profileName"),
                    record.get("review/helpfulness"),
                    float(record.get("review/score", 0)),
                    int(record.get("review/time", 0)),
                    record.get("review/summary"),
                    record.get("review/text")
                ))

            # Step 3: Perform updates within the transaction with progress bar
            print("Updating records within a transaction...")
            update_query = "UPDATE reviews SET score = score + 0.5 WHERE score >= 4.0;"
            cursor.execute("SELECT id FROM reviews WHERE score >= 4.0;")
            ids_to_update = [row[0] for row in cursor.fetchall()]

            for review_id in tqdm(ids_to_update, desc="Updating Records", unit="record"):
                cursor.execute("UPDATE reviews SET score = score + 0.5 WHERE id = %s;", (review_id,))

            # Step 4: Optionally simulate an error to test rollback
            if simulate_error:
                print("Simulating an error to test rollback...")
                raise Exception("Simulated error for rollback test.")

            # Step 5: Commit the transaction
            conn.commit()
            print("Transaction committed successfully.")

        except Exception as e:
            # Step 6: Rollback the transaction in case of an error
            if conn:
                conn.rollback()
            print(f"Transaction failed: {e}. Rolled back changes.")

        finally:
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Session ended. Total execution time: {execution_time:.2f} seconds.")

            if conn:
                cursor.close()
                self.handler._close_connection(conn)

            return execution_time

    # Complex Queries
    def test_complex_query(self):
        """
        Demonstrates a multi-table join or a complex subquery.
        Measures execution time and returns any results for verification.
        """
        print("Testing PostgreSQL complex query...")

        # Adjusted query based on the updated table structures
        query = """
        SELECT r.id AS review_id, r.score, u.name AS user_name, p.product_name
        FROM reviews r
        JOIN users u ON r.user_id = u.user_id
        JOIN products p ON r.product_id = p.product_id
        WHERE r.score > 3.0
        ORDER BY r.score DESC
        LIMIT 100;
        """

        # Measure query execution time
        start_time = time.time()
        conn = self.handler._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(query)
            results = cursor.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")
            results = []
        finally:
            cursor.close()
            self.handler._close_connection(conn)

        end_time = time.time()

        total_time = end_time - start_time
        print(f"Complex query completed in {total_time:.4f} seconds, returned {len(results)} rows.")
        return total_time, results
