import time

from bson import ObjectId
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from db.handler.mongodb_handler import MongoDBHandler
from utils.db_utils import normalize_record


class MongoSimulator:
    def __init__(self, config, use_persistent_connection=False, total_records=None):
        self.total_records = total_records
        self.handler = MongoDBHandler(config)
        self.modified = 0
        self.inserted = 0
        self.deleted = 0

    def setup(self):
        """Set up the database and collection for testing."""
        print("Setting up MongoDB database and collection...")
        self.handler.create_mongo_db()
        self.handler.initialize_collection('reviews')
        print("MongoDB setup complete.")



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

    # Insertion methods
    def test_insertion(self, records):
        self.validate_before_executing("insertion")
        print("Testing MongoDB insertion...")
        start_time = time.time()
        individual_times = []

        for record in tqdm(records, desc="Inserting Records", unit="record"):
            normalized_record = normalize_record(record)
            record_start = time.time()
            self.handler.insert_one('reviews', normalized_record)
            record_end = time.time()
            individual_times.append(record_end - record_start)
            self.inserted += 1

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Inserted {len(records)} records into MongoDB in {total_time:.2f} seconds.")
        return total_time, individual_times

    def test_insertion_many(self, records, bulk_size=-1):
        self.validate_before_executing("insertion")
        print("Testing MongoDB bulk insertion...")
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

    # Update methods
    def test_update_one(self):
        self.validate_before_executing("update")
        print("Testing MongoDB update one...")
        mongo_times = []

        try:
            ids = self.handler.get_all_ids("reviews")
            print(f"Retrieved {len(ids)} IDs from the `reviews` collection.")

            start_time = time.time()
            for doc_id in tqdm(ids, desc="Updating One Document", unit="query"):
                filter_query = {"_id": ObjectId(doc_id)}
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
        print("Testing MongoDB update many with bulk size...")
        mongo_times = []

        try:
            ids = self.handler.get_all_ids("reviews")
            total_ids = len(ids)
            print(f"Retrieved {total_ids} IDs from the `reviews` collection.")

            if bulk_size == -1:
                bulk_size = total_ids
                print("Executing all updates in a single bulk.")

            start_time = time.time()
            for i in tqdm(range(0, total_ids, bulk_size), desc="Updating Many Documents", unit="bulk"):
                bulk_ids = ids[i:i + bulk_size]
                bulk_queries = [{"filter_query": {"_id": ObjectId(doc_id)}} for doc_id in bulk_ids]
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

    def test_concurrent_operations(self, concurrency_level=10, num_operations=100):
        """
        Perform concurrent read, write, and update operations to test MongoDB under load.

        :param concurrency_level: Number of concurrent threads to use.
        :param num_operations: Total number of operations to perform.
        """
        print(
            f"Testing concurrent operations with {concurrency_level} threads and {num_operations} total operations...")

        collection_name = "reviews"

        # Retrieve all IDs for read/update operations
        ids = self.handler.get_all_ids(collection_name)
        if not ids:
            print("No IDs found in the `reviews` collection. Ensure data is inserted before running concurrency tests.")
            return

        # Define tasks: mix of reads, updates, and inserts
        def read_operation():
            random_id = ObjectId(random.choice(ids))
            self.handler.query_one_field(collection_name, "_id", random_id)

        def write_operation():
            record = {
                "product_id": f"Product{random.randint(1, 1000)}",
                "user_id": f"User{random.randint(1, 1000)}",
                "profile_name": f"User{random.randint(1, 1000)}",
                "helpfulness": "0/0",
                "score": random.uniform(1, 5),
                "review_time": int(time.time()),
                "summary": "Sample Summary",
                "review_text": "Sample Review Text"
            }
            self.handler.insert_one(collection_name, record)

        def update_operation():
            random_id = ObjectId(random.choice(ids))
            filter_query = {"_id": random_id}
            update_query = {"$inc": {"score": 0.123}}
            self.handler.update_one(collection_name, filter_query, update_query)

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
        Test transactional operations in MongoDB involving multiple documents.

        :param records: The records to insert within the transaction.
        :param simulate_error: Whether to simulate an error to test rollback behavior.
        :return: Total execution time.
        """
        print("Testing MongoDB multi-document transactional operations...")

        session = self.handler.client.start_session()
        collection_name = 'reviews'
        start_time = time.time()

        try:
            with session.start_transaction():
                print("Transaction started...")

                # Insert multiple records with a progress bar
                print("Inserting records within a transaction...")
                insert_data = [
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
                for i in tqdm(range(0, len(insert_data)), desc="Inserting Records", unit="record"):
                    self.handler.db[collection_name].insert_one(insert_data[i], session=session)

                # Update documents with a progress bar
                print("Updating records within a transaction...")
                update_filter = {"score": {"$gte": 4.0}}
                update_operation = {"$inc": {"score": 0.5}}
                cursor = self.handler.db[collection_name].find(update_filter, session=session)
                ids_to_update = [doc["_id"] for doc in cursor]

                for _id in tqdm(ids_to_update, desc="Updating Records", unit="record"):
                    self.handler.db[collection_name].update_one({"_id": _id}, update_operation, session=session)

                # Optionally simulate an error to test rollback
                if simulate_error:
                    print("Simulating an error to test rollback...")
                    raise Exception("Simulated error for rollback test.")

                # Commit the transaction
                session.commit_transaction()
                print("Transaction committed successfully.")

        except Exception as e:
            # Abort the transaction in case of error
            session.abort_transaction()
            print(f"Transaction aborted due to error: {e}")

        finally:
            session.end_session()
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Session ended. Total execution time: {execution_time:.2f} seconds.")
            return execution_time
