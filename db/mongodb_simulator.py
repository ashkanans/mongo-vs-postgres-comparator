import time

from bson import ObjectId
from tqdm import tqdm

from db.mongodb_handler import MongoDBHandler


class MongoSimulator:
    def __init__(self, config, use_persistent_connection=False, total_records=None):
        self.total_records = total_records
        self.handler = MongoDBHandler(config, use_persistent_connection)
        self.modified = 0
        self.inserted = 0
        self.deleted = 0

    def setup(self):
        """Set up the database and collection for testing."""
        print("Setting up MongoDB database and collection...")
        self.handler.create_mongo_db()
        self.handler.initialize_collection('reviews')
        print("MongoDB setup complete.")

    def test_insertion(self, records):
        self.validate_before_executing("insertion")
        """Test insertion of records into MongoDB with a progress bar."""
        print("Testing MongoDB insertion...")
        start_time = time.time()
        individual_times = []

        # Use tqdm to create a progress bar
        for record in tqdm(records, desc="Inserting Records", unit="record"):
            record_start = time.time()  # Record the start time for this insertion
            self.handler.insert_one('reviews', {
                "product_id": record.get("product/productId"),
                "user_id": record.get("review/userId"),
                "profile_name": record.get("review/profileName"),
                "helpfulness": record.get("review/helpfulness"),
                "score": float(record.get("review/score", 0)),
                "review_time": int(record.get("review/time", 0)),
                "summary": record.get("review/summary"),
                "review_text": record.get("review/text")
            })
            record_end = time.time()
            individual_times.append(record_end - record_start)

        end_time = time.time()
        total_time = end_time - start_time  # Total insertion time
        print(f"Inserted {len(records)} records into MongoDB in {total_time:.2f} seconds.")
        return total_time, individual_times

    def test_query_performance(self, filter_query):
        """Test query performance in MongoDB."""
        print("Testing MongoDB query performance...")
        start_time = time.time()
        results = self.handler.query_multiple_fields('reviews', filter_query)
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Query completed in {total_time:.2f} seconds, returned {len(results)} documents.")
        return total_time, results

    def test_index_performance(self, field):
        """Test performance with and without index."""
        print(f"Testing MongoDB index performance on field '{field}'...")
        # Without Index
        print("Testing without index...")
        filter_query = {field: "some_value"}
        no_index_time, _ = self.test_query_performance(filter_query)

        # With Index
        print("Creating index and testing with index...")
        self.handler.create_single_field_index('reviews', field)
        index_time, _ = self.test_query_performance(filter_query)

        print(f"Performance comparison: Without Index: {no_index_time:.2f}s, With Index: {index_time:.2f}s.")
        return no_index_time, index_time

    def test_insertion_many(self, records, bulk_size=-1):
        self.validate_before_executing("insertion")
        """Test bulk insertion of records into MongoDB."""
        print("Testing MongoDB bulk insertion...")
        start_time = time.time()
        individual_times = []
        total_records = len(records)

        # Determine bulk size
        if bulk_size == -1:
            # Insert all records as one bulk
            bulk_size = total_records
            print("Inserting all records in a single bulk.")

        # Prepare data for insertion
        formatted_records = [
            {
                "product_id": record.get("product/productId"),
                "user_id": record.get("review/userId"),
                "profile_name": record.get("review/profileName"),
                "helpfulness": record.get("review/helpfulness"),
                "score": float(record.get("review/score", 0)),
                "review_time": int(record.get("review/time", 0)),
                "summary": record.get("review/summary"),
                "review_text": record.get("review/text")
            }
            for record in records
        ]

        # Split the data into chunks based on the bulk size
        for i in tqdm(range(0, total_records, bulk_size), desc="Inserting Bulk Records", unit="bulk"):
            bulk_start = time.time()
            bulk = formatted_records[i:i + bulk_size]
            self.inserted += self.handler.insert_many('reviews', bulk)
            bulk_end = time.time()
            individual_times.append(bulk_end - bulk_start)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Inserted {total_records} records into MongoDB in {total_time:.2f} seconds using bulk size {bulk_size}.")
        return total_time, individual_times

    def ensure_empty(self, collection_name="reviews"):
        """Ensure that the MongoDB collection is empty."""
        if not self.handler.is_empty(collection_name):
            print(f"MongoDB collection '{collection_name}' is not empty. Reinitializing...")
            self.handler.initialize_collection(collection_name)
            print(f"MongoDB collection '{collection_name}' has been reinitialized.")

    def test_update_one(self):
        self.validate_before_executing("update")
        """
        Test updating a single document in MongoDB by incrementing the score for each document.
        """
        print("Testing MongoDB update one...")
        mongo_times = []

        try:
            # Step 1: Get all IDs from the 'reviews' collection
            ids = self.handler.get_all_ids("reviews")
            print(f"Retrieved {len(ids)} IDs from the 'reviews' collection.")

            # Step 2: Update each document individually
            start_time = time.time()

            for doc_id in tqdm(ids, desc="Updating One Document", unit="query"):
                filter_query = {"_id": doc_id}
                update_query = {"$inc": {"score": 0.123}}

                op_start = time.time()
                self.handler.update_one("reviews", filter_query, update_query)
                op_end = time.time()

                mongo_times.append(op_end - op_start)

            end_time = time.time()
            mongo_time = end_time - start_time

            print(f"Update one operation completed in {mongo_time:.2f} seconds.")
            return mongo_time, mongo_times

        except Exception as e:
            print(f"Error during the update process: {e}")
            return None, []

    def test_update_many(self, bulk_size=-1):
        self.validate_before_executing("update")
        """
        Test updating multiple documents in MongoDB with bulk execution.
        Fetches all IDs, then performs updates in bulks of the specified size using a single operation per bulk.
        """
        print("Testing MongoDB update many with bulk size...")
        mongo_times = []

        try:
            # Step 1: Get all IDs from the 'reviews' collection
            ids = self.handler.get_all_ids("reviews")
            total_ids = len(ids)
            print(f"Retrieved {total_ids} IDs from the 'reviews' collection.")

            if bulk_size == -1:
                bulk_size = total_ids  # Execute all updates in a single bulk
                print("Executing all updates in a single bulk.")

            # Step 2: Perform updates in bulk
            start_time = time.time()

            for i in tqdm(range(0, total_ids, bulk_size), desc="Updating Many Documents", unit="bulk"):
                bulk_ids = ids[i:i + bulk_size]

                # Generate the bulk update queries (filter queries for all `_id` values in the bulk)
                bulk_queries = [{"filter_query": {"_id": ObjectId(doc_id)}} for doc_id in bulk_ids]

                # Execute the bulk update
                bulk_start = time.time()
                self.handler.update_many_bulk("reviews", bulk_queries)
                bulk_end = time.time()

                mongo_times.append(bulk_end - bulk_start)

            end_time = time.time()
            mongo_time = end_time - start_time

            print(f"Update many operation completed in {mongo_time:.2f} seconds with bulk size {bulk_size}.")
            return mongo_time, mongo_times

        except Exception as e:
            print(f"Error during the bulk update process: {e}")
            return None, []

    def test_delete_one(self):
        self.validate_before_executing("delete")
        """Test deleting a single document in MongoDB based on IDs retrieved from the collection."""
        print("Testing MongoDB delete one...")
        mongo_times = []

        try:
            # Retrieve all IDs from the collection
            delete_ids = self.handler.get_all_ids("reviews")
            total_ids = len(delete_ids)
            print(f"Retrieved {total_ids} IDs for deletion.")

            start_time = time.time()

            for doc_id in tqdm(delete_ids, desc="Deleting One Document", unit="query"):
                filter_query = {"_id": ObjectId(doc_id)}

                op_start = time.time()
                self.handler.delete_one('reviews', filter_query)
                op_end = time.time()

                mongo_times.append(op_end - op_start)

            end_time = time.time()
            mongo_time = end_time - start_time

            print(f"Delete one operation completed in {mongo_time:.2f} seconds.")
            return mongo_time, mongo_times

        except Exception as e:
            print(f"Error during the delete one process: {e}")
            return None, []

    def test_delete_many(self, bulk_size=-1):
        self.validate_before_executing("delete")
        """Test deleting multiple documents in MongoDB with bulk execution."""
        print("Testing MongoDB delete many with bulk size...")
        mongo_times = []

        try:
            # Retrieve all IDs from the collection
            delete_ids = self.handler.get_all_ids("reviews")
            total_ids = len(delete_ids)
            print(f"Retrieved {total_ids} IDs for deletion.")

            if bulk_size == -1:
                bulk_size = total_ids  # Execute all deletes in a single bulk
                print("Executing all delete queries in a single bulk.")

            start_time = time.time()

            for i in tqdm(range(0, total_ids, bulk_size), desc="Deleting Many Documents", unit="bulk"):
                bulk_ids = delete_ids[i:i + bulk_size]

                # Generate the bulk delete queries
                bulk_queries = [{"filter_query": {"_id": ObjectId(doc_id)}} for doc_id in bulk_ids]

                # Execute the bulk delete
                bulk_start = time.time()
                self.handler.delete_many_bulk("reviews", bulk_queries)
                bulk_end = time.time()

                mongo_times.append(bulk_end - bulk_start)

            end_time = time.time()
            mongo_time = end_time - start_time

            print(f"Delete many operation completed in {mongo_time:.2f} seconds with bulk size {bulk_size}.")
            return mongo_time, mongo_times

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
