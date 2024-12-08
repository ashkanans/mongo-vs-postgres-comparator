import time

from tqdm import tqdm

from db.postgres_handler import PostgresDBHandler


class PostgresSimulator:
    def __init__(self, config, use_persistent_connection=False, total_records=None):
        self.total_records = total_records
        self.handler = PostgresDBHandler(config, use_persistent_connection)
        self.modified = 0
        self.inserted = 0
        self.deleted = 0

    def setup(self):
        """Set up the database and table for testing."""
        print("Setting up PostgreSQL database and table...")
        self.handler.create_database()
        self.handler.create_reviews_table()
        print("PostgreSQL setup complete.")

    def test_insertion(self, records):
        self.validate_before_executing("insertion")
        """Test insertion of records into PostgreSQL with a progress bar."""
        print("Testing PostgreSQL insertion...")
        start_time = time.time()
        individual_times = []

        # Use tqdm to create a progress bar
        for record in tqdm(records, desc="Inserting Records", unit="record"):
            record_start = time.time()  # Record the start time for this insertion
            self.handler.insert_one(record)
            self.inserted += 1
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
        self.validate_before_executing("insertion")
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

    def test_update_one(self):
        self.validate_before_executing("update")
        """Test updating a single record in PostgreSQL by incrementing score for each record."""
        print("Testing PostgreSQL update one...")
        postgres_times = []

        try:
            # Step 1: Get all IDs from the reviews table
            ids = self.handler.get_all_review_ids()
            print(f"Retrieved {len(ids)} IDs from the `reviews` table.")

            # Step 2: Update the score for each ID
            start_time = time.time()

            # Use tqdm for progress bar
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
        """
        Test updating multiple records in PostgreSQL with bulk execution.
        Fetches all IDs, then performs updates in bulks of the specified size using a single query per bulk.
        """
        print("Testing PostgreSQL update many with bulk size...")
        postgres_times = []

        try:
            # Step 1: Get all IDs from the `reviews` table
            ids = self.handler.get_all_review_ids()
            total_ids = len(ids)
            print(f"Retrieved {total_ids} IDs from the `reviews` table.")

            if bulk_size == -1:
                bulk_size = total_ids  # Execute all updates in a single bulk
                print("Executing all updates in a single bulk.")

            # Step 2: Perform updates in bulk
            start_time = time.time()

            for i in tqdm(range(0, total_ids, bulk_size), desc="Updating Many Records", unit="bulk"):
                bulk_ids = ids[i:i + bulk_size]

                # Generate the bulk update queries
                bulk_queries = [{"filter_query": (review_id,)} for review_id in bulk_ids]

                # Execute the bulk update
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
